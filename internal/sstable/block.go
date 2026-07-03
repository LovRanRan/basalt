package sstable

import (
	"encoding/binary"
	"math"
)

// blockBuilder assembles one block of prefix-compressed entries. Each entry
// is (shared, unshared, valueLen) uvarints followed by the unshared key
// bytes and the value; every restartInterval-th entry is a restart point
// written with the full key. The block ends with the restart offsets (u32
// each) and their count (u32), which readers use for binary search.
type blockBuilder struct {
	restartInterval int
	buf             []byte
	restarts        []uint32
	counter         int
	lastKey         []byte
	// overflowed is set when the buffer outgrows the u32 restart-offset
	// range; the Writer must fail the table rather than emit a block whose
	// restart array silently wrapped — that damage would carry a valid crc.
	overflowed bool
}

func newBlockBuilder(restartInterval int) *blockBuilder {
	return &blockBuilder{restartInterval: restartInterval}
}

// add appends an entry; the Writer enforces that keys arrive in strictly
// ascending order.
func (b *blockBuilder) add(key, value []byte) {
	if len(b.buf) > math.MaxUint32 {
		b.overflowed = true
		return
	}
	shared := 0
	if b.counter%b.restartInterval == 0 {
		b.restarts = append(b.restarts, uint32(len(b.buf)))
	} else {
		shared = sharedPrefixLen(b.lastKey, key)
	}
	b.buf = binary.AppendUvarint(b.buf, uint64(shared))
	b.buf = binary.AppendUvarint(b.buf, uint64(len(key)-shared))
	b.buf = binary.AppendUvarint(b.buf, uint64(len(value)))
	b.buf = append(b.buf, key[shared:]...)
	b.buf = append(b.buf, value...)
	b.counter++
	b.lastKey = append(b.lastKey[:0], key...)
}

func (b *blockBuilder) estimatedSize() int {
	return len(b.buf) + 4*len(b.restarts) + 4
}

// finish appends the restart array and its count; the returned slice is
// owned by the builder until reset.
func (b *blockBuilder) finish() []byte {
	// An empty block still encodes one restart at offset 0 pointing at the
	// (empty) entry region — intentional and golden-pinned; readers must
	// clamp restart offsets to the entry region before dereferencing.
	if len(b.restarts) == 0 {
		b.restarts = append(b.restarts, 0)
	}
	for _, r := range b.restarts {
		b.buf = binary.LittleEndian.AppendUint32(b.buf, r)
	}
	b.buf = binary.LittleEndian.AppendUint32(b.buf, uint32(len(b.restarts)))
	return b.buf
}

func (b *blockBuilder) reset() {
	b.buf = b.buf[:0]
	b.restarts = b.restarts[:0]
	b.counter = 0
	b.lastKey = b.lastKey[:0]
	b.overflowed = false
}

func (b *blockBuilder) empty() bool { return b.counter == 0 }

func sharedPrefixLen(a, b []byte) int {
	n := min(len(a), len(b))
	i := 0
	for i < n && a[i] == b[i] {
		i++
	}
	return i
}
