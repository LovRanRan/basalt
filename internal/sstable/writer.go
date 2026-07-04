package sstable

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/LovRanRan/basalt/internal/base"
)

// ErrOutOfOrder rejects an Add whose key is not strictly greater than every
// previously added key; an exact duplicate is out of order too.
var ErrOutOfOrder = errors.New("sstable: keys must be added in strictly ascending order")

var (
	errBlockOverflow = errors.New("sstable: block exceeds u32 restart-offset range")
	errBloomOverflow = errors.New("sstable: too many keys for a single bloom filter")
)

type WriterOptions struct {
	// BlockSize is the target data-block size including the restart array;
	// a block is cut once its encoded size reaches this. Default 4096,
	// capped at 1 GiB to keep restart offsets within their u32 encoding.
	BlockSize int
	// RestartInterval is the number of entries between restart points in
	// data blocks. Default 16.
	RestartInterval int
	// BitsPerKey sizes the bloom filter. Default 10 (≈1% false positives).
	BitsPerKey int
}

func (o *WriterOptions) defaults() {
	if o.BlockSize <= 0 {
		o.BlockSize = 4096
	}
	if o.BlockSize > 1<<30 {
		o.BlockSize = 1 << 30
	}
	if o.RestartInterval <= 0 {
		o.RestartInterval = 16
	}
	if o.BitsPerKey <= 0 {
		o.BitsPerKey = 10
	}
}

// Properties are collected while writing; the manifest records them.
type Properties struct {
	NumEntries uint64
	Smallest   []byte // first internal key, nil for an empty table
	Largest    []byte // last internal key, nil for an empty table
	FileSize   uint64
}

// Writer streams sorted internal keys into a table. After any error the
// writer is permanently failed — a partially written table must be
// discarded, never linked into the manifest. Not safe for concurrent use.
type Writer struct {
	dst  io.Writer
	opts WriterOptions

	data  *blockBuilder
	index *blockBuilder

	offset  uint64
	hashes  []uint32
	props   Properties
	lastKey []byte
	err     error
	done    bool
}

func NewWriter(dst io.Writer, opts WriterOptions) *Writer {
	opts.defaults()
	return &Writer{
		dst:  dst,
		opts: opts,
		data: newBlockBuilder(opts.RestartInterval),
		// Every index entry is its own restart point, so readers binary
		// search the index directly.
		index: newBlockBuilder(1),
	}
}

func (w *Writer) fail(err error) error {
	if w.err == nil {
		w.err = err
	}
	return err
}

// Add appends an entry. key is an encoded internal key, strictly greater
// (per base.Compare) than every key added before it; both slices are copied
// as needed and may be reused by the caller.
func (w *Writer) Add(key, value []byte) error {
	if w.err != nil {
		return w.err
	}
	if w.done {
		return errors.New("sstable: writer already finished")
	}
	if len(key) < base.TrailerLen {
		return w.fail(fmt.Errorf("sstable: key %x shorter than internal-key trailer", key))
	}
	if w.props.NumEntries > 0 && base.Compare(key, w.lastKey) <= 0 {
		return w.fail(fmt.Errorf("%w: %x after %x", ErrOutOfOrder, key, w.lastKey))
	}
	if w.props.NumEntries == 0 {
		w.props.Smallest = append([]byte(nil), key...)
	}
	w.lastKey = append(w.lastKey[:0], key...)
	w.hashes = append(w.hashes, leveldbHash(base.UserKey(key)))
	w.data.add(key, value)
	if w.data.overflowed {
		return w.fail(errBlockOverflow)
	}
	w.props.NumEntries++
	if w.data.estimatedSize() >= w.opts.BlockSize {
		return w.flushDataBlock()
	}
	return nil
}

// flushDataBlock writes the pending data block and indexes it under its
// last key.
func (w *Writer) flushDataBlock() error {
	if w.data.empty() {
		return nil
	}
	h, err := w.writeBlock(w.data.finish())
	if err != nil {
		return w.fail(err)
	}
	w.index.add(w.lastKey, h.append(nil))
	if w.index.overflowed {
		return w.fail(errBlockOverflow)
	}
	w.data.reset()
	return nil
}

// writeBlock writes contents followed by its crc32c trailer.
func (w *Writer) writeBlock(contents []byte) (blockHandle, error) {
	h := blockHandle{offset: w.offset, length: uint64(len(contents))}
	var trailer [blockTrailerLen]byte
	binary.LittleEndian.PutUint32(trailer[:], crc32.Checksum(contents, castagnoli))
	if _, err := w.dst.Write(contents); err != nil {
		return blockHandle{}, err
	}
	if _, err := w.dst.Write(trailer[:]); err != nil {
		return blockHandle{}, err
	}
	w.offset += uint64(len(contents)) + blockTrailerLen
	return h, nil
}

// EstimatedSize returns the table's projected on-disk size if finished
// now: bytes already written plus the pending data and index blocks.
// Compaction uses it to split output files at a target size.
func (w *Writer) EstimatedSize() uint64 {
	return w.offset + uint64(w.data.estimatedSize()) + uint64(w.index.estimatedSize())
}

// Finish flushes the pending data block, writes the filter block, index
// block, and footer, and returns the table's properties. The caller owns
// syncing and durably naming the underlying file.
func (w *Writer) Finish() (Properties, error) {
	if w.err != nil {
		return Properties{}, w.err
	}
	if w.done {
		return Properties{}, errors.New("sstable: writer already finished")
	}
	w.done = true
	if err := w.flushDataBlock(); err != nil {
		return Properties{}, err
	}
	// The byte-rounded bloom bit count must fit u32 arithmetic; beyond it
	// the filter would silently degrade (wrapped modulus) or divide by
	// zero.
	if bits := uint64(len(w.hashes)) * uint64(w.opts.BitsPerKey); (bits+7)/8*8 >= 1<<32 {
		return Properties{}, w.fail(errBloomOverflow)
	}
	filterHandle, err := w.writeBlock(buildBloom(w.hashes, w.opts.BitsPerKey))
	if err != nil {
		return Properties{}, w.fail(err)
	}
	indexHandle, err := w.writeBlock(w.index.finish())
	if err != nil {
		return Properties{}, w.fail(err)
	}
	if _, err := w.dst.Write(footer{filter: filterHandle, index: indexHandle}.encode()); err != nil {
		return Properties{}, w.fail(err)
	}
	w.offset += footerLen
	if w.props.NumEntries > 0 {
		w.props.Largest = append([]byte(nil), w.lastKey...)
	}
	w.props.FileSize = w.offset
	return w.props, nil
}
