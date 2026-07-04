package sstable

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"sort"

	"github.com/LovRanRan/basalt/internal/base"
)

// ErrCorruption reports a structurally invalid or checksum-failing table;
// the message identifies the damaged region.
var ErrCorruption = errors.New("sstable: corruption")

func corruptf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrCorruption, fmt.Sprintf(format, args...))
}

// Reader serves point gets and iteration over one immutable table. The
// footer, index, and filter are validated eagerly at open; data blocks are
// read lazily and checksum-verified per access. A Reader is immutable and
// safe for concurrent use as long as src.ReadAt is (os.File is).
type Reader struct {
	src    io.ReaderAt
	size   int64
	index  []indexEntry
	filter []byte
}

type indexEntry struct {
	lastKey []byte
	handle  blockHandle
}

func NewReader(src io.ReaderAt, size int64) (*Reader, error) {
	if size < footerLen {
		return nil, corruptf("file of %d bytes cannot hold a footer", size)
	}
	var ftrBuf [footerLen]byte
	if _, err := src.ReadAt(ftrBuf[:], size-footerLen); err != nil {
		return nil, fmt.Errorf("sstable: read footer: %w", err)
	}
	ftr, err := decodeFooter(ftrBuf[:])
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrCorruption, err)
	}
	r := &Reader{src: src, size: size}
	if r.filter, err = r.readBlock(ftr.filter, "filter"); err != nil {
		return nil, err
	}
	indexContents, err := r.readBlock(ftr.index, "index")
	if err != nil {
		return nil, err
	}
	if err := r.parseIndexBlock(indexContents); err != nil {
		return nil, err
	}
	return r, nil
}

// readBlock reads and checksum-verifies one block.
func (r *Reader) readBlock(h blockHandle, region string) ([]byte, error) {
	if h.length > math.MaxInt32 {
		return nil, corruptf("%s handle length %d absurd", region, h.length)
	}
	end := h.offset + h.length + blockTrailerLen
	if end < h.offset || end > uint64(r.size)-footerLen {
		return nil, corruptf("%s handle [%d,%d) out of range", region, h.offset, end)
	}
	buf := make([]byte, h.length+blockTrailerLen)
	if _, err := r.src.ReadAt(buf, int64(h.offset)); err != nil {
		return nil, fmt.Errorf("sstable: read %s block: %w", region, err)
	}
	contents := buf[:h.length:h.length]
	want := binary.LittleEndian.Uint32(buf[h.length:])
	if crc32.Checksum(contents, castagnoli) != want {
		return nil, corruptf("%s block checksum mismatch", region)
	}
	return contents, nil
}

// parseIndexBlock decodes the index eagerly: every entry is a full key (the
// writer uses restart interval 1) mapping a data block's last key to its
// handle. Keys must ascend and handles must stay in range.
func (r *Reader) parseIndexBlock(contents []byte) error {
	pb, err := parseBlock(contents, "index")
	if err != nil {
		return err
	}
	it := newBlockIter(pb, "index")
	for it.first(); it.valid; it.decodeNext() {
		// Index entries must be full keys (writer uses restart interval
		// 1); prefix-extended entries would make the eager per-entry key
		// copies quadratic in block size — an allocation amplifier.
		if it.lastShared != 0 {
			return corruptf("index entry is not a full key")
		}
		h, rest, ok := decodeBlockHandle(it.value)
		if !ok || len(rest) != 0 {
			return corruptf("bad block handle in index")
		}
		end := h.offset + h.length + blockTrailerLen
		if end < h.offset || end > uint64(r.size)-footerLen {
			return corruptf("data block handle out of range")
		}
		key := append([]byte(nil), it.key...)
		if n := len(r.index); n > 0 && base.Compare(r.index[n-1].lastKey, key) >= 0 {
			return corruptf("index keys not ascending")
		}
		r.index = append(r.index, indexEntry{lastKey: key, handle: h})
	}
	if it.err != nil {
		return it.err
	}
	emptyOK := len(r.index) == 0 && len(pb.restarts) == 1
	if len(r.index) != len(pb.restarts) && !emptyOK {
		return corruptf("index restart count %d does not match %d entries", len(pb.restarts), len(r.index))
	}
	return nil
}

// seekIndex returns the position of the first data block whose last key is
// >= target, or len(index) when the target is past the whole table.
func (r *Reader) seekIndex(target []byte) int {
	return sort.Search(len(r.index), func(i int) bool {
		return base.Compare(r.index[i].lastKey, target) >= 0
	})
}

// Get returns the newest entry for userKey visible at seq. ok=false with a
// nil error means the key is absent; a found KindDelete is a tombstone and
// is the caller's to interpret. The returned value is freshly allocated per
// call and owned by the caller.
func (r *Reader) Get(userKey []byte, seq uint64) (value []byte, kind base.Kind, ok bool, err error) {
	if !bloomMayContain(r.filter, leveldbHash(userKey)) {
		return nil, 0, false, nil
	}
	seek := base.AppendSeekKey(nil, userKey, seq)
	i := r.seekIndex(seek)
	if i == len(r.index) {
		return nil, 0, false, nil
	}
	contents, err := r.readBlock(r.index[i].handle, fmt.Sprintf("data[%d]", i))
	if err != nil {
		return nil, 0, false, err
	}
	pb, err := parseBlock(contents, fmt.Sprintf("data[%d]", i))
	if err != nil {
		return nil, 0, false, err
	}
	it := newBlockIter(pb, fmt.Sprintf("data[%d]", i))
	it.seekGE(seek)
	if it.err != nil {
		return nil, 0, false, it.err
	}
	if !it.valid {
		// seekIndex proved this block's last key is >= seek, so an
		// exhausted seek means the index and block disagree.
		return nil, 0, false, corruptf("data[%d]: no entry >= its index key", i)
	}
	ik, err := base.DecodeInternalKey(it.key)
	if err != nil {
		return nil, 0, false, corruptf("data[%d]: %v", i, err)
	}
	if string(ik.UserKey) != string(userKey) {
		return nil, 0, false, nil
	}
	return append([]byte(nil), it.value...), ik.Kind, true, nil
}
