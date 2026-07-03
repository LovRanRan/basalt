package sstable

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestBlockBuilderPrefixCompressionRoundtrip(t *testing.T) {
	b := newBlockBuilder(2)
	keys := []string{"apple", "applet", "apply", "banana", "band"}
	for i, k := range keys {
		b.add([]byte(k), []byte{byte(i)})
	}
	contents := b.finish()

	nRestarts := int(binary.LittleEndian.Uint32(contents[len(contents)-4:]))
	if nRestarts != 3 {
		t.Fatalf("restarts = %d, want 3 (entries 0, 2, 4)", nRestarts)
	}
	arrStart := len(contents) - 4 - 4*nRestarts
	restarts := make([]uint32, nRestarts)
	for i := range restarts {
		restarts[i] = binary.LittleEndian.Uint32(contents[arrStart+4*i:])
	}
	if restarts[0] != 0 {
		t.Fatalf("first restart = %d, want 0", restarts[0])
	}

	rest := contents[:arrStart]
	offset := 0
	var prev []byte
	var gotKeys []string
	var gotVals []byte
	entryOffsets := map[int]bool{}
	compressed := false
	for len(rest) > 0 {
		entryOffsets[offset] = true
		start := len(rest)
		shared, n1 := binary.Uvarint(rest)
		rest = rest[n1:]
		unshared, n2 := binary.Uvarint(rest)
		rest = rest[n2:]
		vlen, n3 := binary.Uvarint(rest)
		rest = rest[n3:]
		if shared > 0 {
			compressed = true
		}
		key := append(append([]byte(nil), prev[:shared]...), rest[:unshared]...)
		rest = rest[unshared:]
		gotVals = append(gotVals, rest[:vlen]...)
		rest = rest[vlen:]
		gotKeys = append(gotKeys, string(key))
		prev = key
		offset += start - len(rest)
	}

	for i, k := range keys {
		if gotKeys[i] != k {
			t.Fatalf("key %d = %q, want %q", i, gotKeys[i], k)
		}
		if gotVals[i] != byte(i) {
			t.Fatalf("value %d = %d, want %d", i, gotVals[i], i)
		}
	}
	if !compressed {
		t.Fatal("no entry used prefix compression")
	}
	for _, r := range restarts {
		if !entryOffsets[int(r)] {
			t.Fatalf("restart offset %d is not an entry boundary", r)
		}
	}

	b.reset()
	if !b.empty() {
		t.Fatal("builder not empty after reset")
	}
}

func TestSharedPrefixLen(t *testing.T) {
	cases := []struct {
		a, b string
		want int
	}{
		{"", "", 0},
		{"a", "", 0},
		{"abc", "abd", 2},
		{"abc", "abc", 3},
		{"abc", "abcdef", 3},
	}
	for _, tc := range cases {
		if got := sharedPrefixLen([]byte(tc.a), []byte(tc.b)); got != tc.want {
			t.Errorf("sharedPrefixLen(%q, %q) = %d, want %d", tc.a, tc.b, got, tc.want)
		}
	}
}

func TestEmptyBlockFinish(t *testing.T) {
	b := newBlockBuilder(16)
	contents := b.finish()
	if n := binary.LittleEndian.Uint32(contents[len(contents)-4:]); n != 1 {
		t.Fatalf("empty block restarts = %d, want 1", n)
	}
	if !bytes.Equal(contents[:len(contents)-8], nil) {
		t.Fatal("empty block must have no entry bytes")
	}
}
