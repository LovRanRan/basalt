// Package sstable implements the on-disk table format: prefix-compressed
// data blocks, a bloom filter over user keys, an index block mapping each
// data block's last key to its handle, and a fixed-length checksummed
// footer. Every block carries a crc32c trailer.
package sstable

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

const (
	// tableMagic and formatVersion are pinned by the golden byte-layout
	// test; bump the version for any format change.
	tableMagic    uint64 = 0xba5a17557ab1e000
	formatVersion uint32 = 1

	// footerLen is the fixed footer size: filter handle (2×u64), index
	// handle (2×u64), version (u32), crc32c over the preceding 36 bytes
	// (u32), magic (u64).
	footerLen = 48

	// blockTrailerLen is the crc32c trailer following every block.
	blockTrailerLen = 4
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// blockHandle locates a block: offset of its contents in the file and the
// contents length excluding the crc trailer.
type blockHandle struct {
	offset uint64
	length uint64
}

func (h blockHandle) append(dst []byte) []byte {
	dst = binary.AppendUvarint(dst, h.offset)
	return binary.AppendUvarint(dst, h.length)
}

func decodeBlockHandle(buf []byte) (h blockHandle, rest []byte, ok bool) {
	off, n := binary.Uvarint(buf)
	if n <= 0 {
		return blockHandle{}, nil, false
	}
	buf = buf[n:]
	length, n := binary.Uvarint(buf)
	if n <= 0 {
		return blockHandle{}, nil, false
	}
	return blockHandle{offset: off, length: length}, buf[n:], true
}

type footer struct {
	filter blockHandle
	index  blockHandle
}

func (f footer) encode() []byte {
	buf := make([]byte, footerLen)
	binary.LittleEndian.PutUint64(buf[0:], f.filter.offset)
	binary.LittleEndian.PutUint64(buf[8:], f.filter.length)
	binary.LittleEndian.PutUint64(buf[16:], f.index.offset)
	binary.LittleEndian.PutUint64(buf[24:], f.index.length)
	binary.LittleEndian.PutUint32(buf[32:], formatVersion)
	binary.LittleEndian.PutUint32(buf[36:], crc32.Checksum(buf[:36], castagnoli))
	binary.LittleEndian.PutUint64(buf[40:], tableMagic)
	return buf
}

var (
	errBadMagic   = errors.New("sstable: bad magic — not an sstable")
	errBadVersion = errors.New("sstable: unsupported format version")
	errBadFooter  = errors.New("sstable: footer checksum mismatch")
)

func decodeFooter(buf []byte) (footer, error) {
	if len(buf) != footerLen {
		return footer{}, errBadFooter
	}
	if binary.LittleEndian.Uint64(buf[40:]) != tableMagic {
		return footer{}, errBadMagic
	}
	if binary.LittleEndian.Uint32(buf[36:40]) != crc32.Checksum(buf[:36], castagnoli) {
		return footer{}, errBadFooter
	}
	if binary.LittleEndian.Uint32(buf[32:36]) != formatVersion {
		return footer{}, errBadVersion
	}
	return footer{
		filter: blockHandle{binary.LittleEndian.Uint64(buf[0:]), binary.LittleEndian.Uint64(buf[8:])},
		index:  blockHandle{binary.LittleEndian.Uint64(buf[16:]), binary.LittleEndian.Uint64(buf[24:])},
	}, nil
}
