package sstable

import (
	"encoding/binary"
	"errors"
	"testing"

	"github.com/LovRanRan/basalt/internal/base"
)

func entryBytes(shared, unshared, vlen uint64, tail []byte) []byte {
	var e []byte
	e = binary.AppendUvarint(e, shared)
	e = binary.AppendUvarint(e, unshared)
	e = binary.AppendUvarint(e, vlen)
	return append(e, tail...)
}

func rawBlock(entries [][]byte, restarts []uint32) []byte {
	var b []byte
	for _, e := range entries {
		b = append(b, e...)
	}
	for _, r := range restarts {
		b = binary.LittleEndian.AppendUint32(b, r)
	}
	return binary.LittleEndian.AppendUint32(b, uint32(len(restarts)))
}

// driveBlock exercises every blockIter path over crafted contents and
// returns the first error surfaced. Hostile input must produce
// ErrCorruption — never a panic, and never a clean pass.
func driveBlock(contents []byte) error {
	pb, err := parseBlock(contents, "test")
	if err != nil {
		return err
	}
	it := newBlockIter(pb, "test")
	for it.first(); it.valid; it.next() {
	}
	if it.err != nil {
		return it.err
	}
	it2 := newBlockIter(pb, "test")
	it2.seekGE(base.AppendSeekKey(nil, []byte("m"), base.MaxSeq))
	for it2.valid {
		it2.next()
	}
	return it2.err
}

func TestHostileBlocksAreCorruptionNotPanic(t *testing.T) {
	validKey := ik("aaaa", 1, base.KindPut) // 12 bytes
	validEntry := entryBytes(0, uint64(len(validKey)), 1, append(append([]byte(nil), validKey...), 'v'))

	cases := []struct {
		name     string
		contents []byte
	}{
		{"restart key shorter than trailer", rawBlock([][]byte{entryBytes(0, 3, 0, []byte("abc"))}, []uint32{0})},
		{"empty restart key", rawBlock([][]byte{entryBytes(0, 0, 0, nil)}, []uint32{0})},
		{"unshared uvarint overflow", rawBlock([][]byte{entryBytes(0, ^uint64(0), 1, []byte("x"))}, []uint32{0})},
		{"vlen uvarint overflow", rawBlock([][]byte{entryBytes(0, uint64(len(validKey)), ^uint64(0), append(append([]byte(nil), validKey...), 'v'))}, []uint32{0})},
		{"shared exceeds previous key", rawBlock([][]byte{validEntry, entryBytes(50, 1, 0, []byte("x"))}, []uint32{0})},
		{"truncated entry header", rawBlock([][]byte{{0x80}}, []uint32{0})},
		{"restart count zero", binary.LittleEndian.AppendUint32(nil, 0)},
		{"restart count outruns block", binary.LittleEndian.AppendUint32(nil, 1<<20)},
		{"restart offsets not increasing", rawBlock([][]byte{validEntry, validEntry}, []uint32{0, 0})},
		{"restart offset past entries", rawBlock([][]byte{validEntry}, []uint32{200})},
		{"restart mid-block at entries end", rawBlock([][]byte{validEntry}, []uint32{uint32(len(validEntry))})},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := driveBlock(tc.contents)
			if !errors.Is(err, ErrCorruption) {
				t.Fatalf("err = %v, want ErrCorruption", err)
			}
		})
	}
}

func TestHostileIndexRejected(t *testing.T) {
	hv := blockHandle{offset: 0, length: 16}.append(nil)
	full := entryBytes(0, uint64(len(ik("aaaa", 9, base.KindPut))), uint64(len(hv)), append(ik("aaaa", 9, base.KindPut), hv...))
	extended := entryBytes(uint64(len(ik("aaaa", 9, base.KindPut))-1), 9, uint64(len(hv)), append(ik("b", 8, base.KindPut), hv...))

	r := &Reader{size: 1 << 20}
	err := r.parseIndexBlock(rawBlock([][]byte{full, extended}, []uint32{0, uint32(len(full))}))
	if !errors.Is(err, ErrCorruption) {
		t.Fatalf("prefix-extended index entry: err = %v, want ErrCorruption", err)
	}

	r2 := &Reader{size: 64}
	wild := blockHandle{offset: ^uint64(0) - 8, length: 16}.append(nil)
	entry := entryBytes(0, uint64(len(ik("k", 1, base.KindPut))), uint64(len(wild)), append(ik("k", 1, base.KindPut), wild...))
	err = r2.parseIndexBlock(rawBlock([][]byte{entry}, []uint32{0}))
	if !errors.Is(err, ErrCorruption) {
		t.Fatalf("wrapping handle: err = %v, want ErrCorruption", err)
	}
}
