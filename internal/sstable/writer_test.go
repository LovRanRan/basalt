package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"hash/crc32"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/LovRanRan/basalt/internal/base"
)

var update = flag.Bool("update", false, "rewrite golden files")

func ik(uk string, seq uint64, kind base.Kind) []byte {
	return base.AppendInternalKey(nil, []byte(uk), seq, kind)
}

// buildGolden writes the fixed table the golden test pins: small blocks to
// force several data blocks, at least one block with multiple restart
// points, a block cut between two versions of the same user key,
// prefix-heavy keys, a tombstone, and an empty value.
func buildGolden(t *testing.T) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := NewWriter(&buf, WriterOptions{BlockSize: 64, RestartInterval: 2, BitsPerKey: 10})
	entries := []struct {
		key   []byte
		value string
	}{
		{ik("user:0001", 9, base.KindPut), "alice"},
		{ik("user:0001", 4, base.KindPut), "al"},
		{ik("user:0002", 7, base.KindDelete), ""},
		{ik("user:0002", 3, base.KindPut), "bob"},
		{ik("user:0002", 2, base.KindPut), "b2"},
		{ik("user:0010", 12, base.KindPut), "carol-with-a-longer-value"},
		{ik("user:0011", 13, base.KindPut), "dan"},
		{ik("user:0020", 2, base.KindPut), ""},
		{ik("zzz", 1, base.KindPut), "end"},
	}
	for _, e := range entries {
		if err := w.Add(e.key, []byte(e.value)); err != nil {
			t.Fatalf("add %q: %v", e.key, err)
		}
	}
	if _, err := w.Finish(); err != nil {
		t.Fatalf("finish: %v", err)
	}
	return buf.Bytes()
}

func TestGoldenByteLayout(t *testing.T) {
	got := buildGolden(t)
	golden := filepath.Join("testdata", "golden.sst")
	if *update {
		if err := os.MkdirAll("testdata", 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(golden, got, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(golden)
	if err != nil {
		t.Fatalf("read golden (regenerate with -update): %v", err)
	}
	if !bytes.Equal(got, want) {
		i := 0
		for i < len(got) && i < len(want) && got[i] == want[i] {
			i++
		}
		t.Fatalf("byte layout drifted: len %d vs golden %d, first difference at offset %d", len(got), len(want), i)
	}
}

// checkBlock validates a handle's range and crc trailer, returning the
// block contents.
func checkBlock(t *testing.T, table []byte, h blockHandle, name string) []byte {
	t.Helper()
	end := h.offset + h.length
	if end+blockTrailerLen > uint64(len(table)) {
		t.Fatalf("%s handle [%d,%d) out of range", name, h.offset, end)
	}
	contents := table[h.offset:end]
	wantCRC := binary.LittleEndian.Uint32(table[end : end+blockTrailerLen])
	if crc32.Checksum(contents, castagnoli) != wantCRC {
		t.Fatalf("%s block crc mismatch", name)
	}
	return contents
}

type indexEntry struct {
	key    []byte
	handle blockHandle
}

// parseIndex validates the footer and index block and returns the index
// entries. The index uses restart interval 1: every entry is a full key.
func parseIndex(t *testing.T, table []byte) []indexEntry {
	t.Helper()
	ftr, err := decodeFooter(table[len(table)-footerLen:])
	if err != nil {
		t.Fatal(err)
	}
	index := checkBlock(t, table, ftr.index, "index")
	nRestarts := int(binary.LittleEndian.Uint32(index[len(index)-4:]))
	entriesEnd := len(index) - 4 - 4*nRestarts
	rest := index[:entriesEnd]
	var entries []indexEntry
	for len(rest) > 0 {
		shared, n1 := binary.Uvarint(rest)
		rest = rest[n1:]
		unshared, n2 := binary.Uvarint(rest)
		rest = rest[n2:]
		vlen, n3 := binary.Uvarint(rest)
		rest = rest[n3:]
		if shared != 0 {
			t.Fatal("index entries must carry full keys")
		}
		key := append([]byte(nil), rest[:unshared]...)
		rest = rest[unshared:]
		h, _, ok := decodeBlockHandle(rest[:vlen])
		rest = rest[vlen:]
		if !ok {
			t.Fatal("bad block handle in index")
		}
		entries = append(entries, indexEntry{key: key, handle: h})
	}
	if nRestarts != len(entries) {
		t.Fatalf("index restarts = %d, want one per entry (%d)", nRestarts, len(entries))
	}
	return entries
}

// decodeFirstKey returns a data block's first key, which is always a full
// (restart) key.
func decodeFirstKey(t *testing.T, block []byte) []byte {
	t.Helper()
	nRestarts := int(binary.LittleEndian.Uint32(block[len(block)-4:]))
	rest := block[:len(block)-4-4*nRestarts]
	if len(rest) == 0 {
		t.Fatal("empty data block")
	}
	shared, n1 := binary.Uvarint(rest)
	rest = rest[n1:]
	unshared, n2 := binary.Uvarint(rest)
	rest = rest[n2:]
	_, n3 := binary.Uvarint(rest)
	rest = rest[n3:]
	if shared != 0 {
		t.Fatal("first entry must be a full key")
	}
	return append([]byte(nil), rest[:unshared]...)
}

func TestTableStructure(t *testing.T) {
	table := buildGolden(t)
	ftr, err := decodeFooter(table[len(table)-footerLen:])
	if err != nil {
		t.Fatal(err)
	}
	filter := checkBlock(t, table, ftr.filter, "filter")
	if k := filter[len(filter)-1]; k < 1 || k > 30 {
		t.Fatalf("bloom probe count = %d", k)
	}

	entries := parseIndex(t, table)
	if len(entries) < 2 {
		t.Fatalf("expected multiple data blocks, got %d", len(entries))
	}
	multiRestart := false
	versionStraddle := false
	for i, e := range entries {
		blockData := checkBlock(t, table, e.handle, fmt.Sprintf("data[%d]", i))
		if binary.LittleEndian.Uint32(blockData[len(blockData)-4:]) > 1 {
			multiRestart = true
		}
		if i > 0 {
			if base.Compare(entries[i-1].key, e.key) >= 0 {
				t.Fatal("index keys not strictly ascending")
			}
			firstKey := decodeFirstKey(t, blockData)
			if bytes.Equal(base.UserKey(entries[i-1].key), base.UserKey(firstKey)) {
				versionStraddle = true
			}
		}
	}
	if !multiRestart {
		t.Fatal("golden must pin a data block with more than one restart point")
	}
	if !versionStraddle {
		t.Fatal("golden must pin a block cut between versions of one user key")
	}
	if want := ik("zzz", 1, base.KindPut); !bytes.Equal(entries[len(entries)-1].key, want) {
		t.Fatalf("last index key = %x, want the table's largest %x", entries[len(entries)-1].key, want)
	}
}

func TestPropertiesRandomized(t *testing.T) {
	rng := rand.New(rand.NewSource(0xba5a17))
	var buf bytes.Buffer
	w := NewWriter(&buf, WriterOptions{BlockSize: 512})
	const n = 2000
	var first, last []byte
	for i := 0; i < n; i++ {
		key := ik(fmt.Sprintf("key-%06d", i), uint64(i+1), base.KindPut)
		if err := w.Add(key, bytes.Repeat([]byte{byte(i)}, rng.Intn(50))); err != nil {
			t.Fatal(err)
		}
		if first == nil {
			first = key
		}
		last = key
	}
	props, err := w.Finish()
	if err != nil {
		t.Fatal(err)
	}
	if props.NumEntries != n {
		t.Fatalf("NumEntries = %d, want %d", props.NumEntries, n)
	}
	if !bytes.Equal(props.Smallest, first) {
		t.Fatalf("Smallest = %x, want %x", props.Smallest, first)
	}
	if !bytes.Equal(props.Largest, last) {
		t.Fatalf("Largest = %x, want %x", props.Largest, last)
	}
	if props.FileSize != uint64(buf.Len()) {
		t.Fatalf("FileSize = %d, actual %d", props.FileSize, buf.Len())
	}
	table := buf.Bytes()
	ftr, err := decodeFooter(table[len(table)-footerLen:])
	if err != nil {
		t.Fatal(err)
	}
	checkBlock(t, table, ftr.filter, "filter")
	checkBlock(t, table, ftr.index, "index")
}

func TestOutOfOrderRejectedAndSticky(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf, WriterOptions{})
	if err := w.Add(ik("b", 5, base.KindPut), nil); err != nil {
		t.Fatal(err)
	}
	if err := w.Add(ik("a", 9, base.KindPut), nil); !errors.Is(err, ErrOutOfOrder) {
		t.Fatalf("smaller user key = %v, want ErrOutOfOrder", err)
	}
	if err := w.Add(ik("z", 1, base.KindPut), nil); !errors.Is(err, ErrOutOfOrder) {
		t.Fatalf("writer must stay failed, got %v", err)
	}
	if _, err := w.Finish(); !errors.Is(err, ErrOutOfOrder) {
		t.Fatalf("finish after failure = %v, want sticky ErrOutOfOrder", err)
	}
}

func TestOrderViolationsRejected(t *testing.T) {
	cases := []struct {
		name string
		a, b []byte
	}{
		{"exact duplicate", ik("k", 5, base.KindPut), ik("k", 5, base.KindPut)},
		{"same user key ascending seq", ik("k", 1, base.KindPut), ik("k", 2, base.KindPut)},
		{"same user key and seq, ascending kind", ik("k", 5, base.KindDelete), ik("k", 5, base.KindPut)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			w := NewWriter(&buf, WriterOptions{})
			if err := w.Add(tc.a, nil); err != nil {
				t.Fatal(err)
			}
			if err := w.Add(tc.b, nil); !errors.Is(err, ErrOutOfOrder) {
				t.Fatalf("err = %v, want ErrOutOfOrder", err)
			}
		})
	}
}

func TestShortKeyRejected(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf, WriterOptions{})
	if err := w.Add([]byte("short"), nil); err == nil || errors.Is(err, ErrOutOfOrder) {
		t.Fatalf("short key = %v, want a non-order typed failure", err)
	}
	if err := w.Add(ik("ok", 1, base.KindPut), nil); err == nil {
		t.Fatal("writer must stay failed after a short key")
	}
}

func TestFinishSemantics(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf, WriterOptions{})
	props, err := w.Finish()
	if err != nil {
		t.Fatal(err)
	}
	if props.NumEntries != 0 || props.Smallest != nil || props.Largest != nil {
		t.Fatalf("empty table props = %+v", props)
	}
	if props.FileSize != uint64(buf.Len()) {
		t.Fatalf("FileSize = %d, actual %d", props.FileSize, buf.Len())
	}
	if _, err := decodeFooter(buf.Bytes()[buf.Len()-footerLen:]); err != nil {
		t.Fatalf("empty table footer: %v", err)
	}
	if _, err := w.Finish(); err == nil {
		t.Fatal("second Finish must fail")
	}
	if err := w.Add(ik("a", 1, base.KindPut), nil); err == nil {
		t.Fatal("Add after Finish must fail")
	}
}

// errWriter accepts n bytes then fails every write.
type errWriter struct {
	n int
}

var errBoom = errors.New("boom")

func (e *errWriter) Write(p []byte) (int, error) {
	if len(p) > e.n {
		n := e.n
		e.n = 0
		return n, errBoom
	}
	e.n -= len(p)
	return len(p), nil
}

func TestWriterStickyAfterIOFailure(t *testing.T) {
	t.Run("flush fails during Add", func(t *testing.T) {
		w := NewWriter(&errWriter{n: 0}, WriterOptions{BlockSize: 32})
		var err error
		for i := 0; i < 10 && err == nil; i++ {
			err = w.Add(ik(fmt.Sprintf("key-%02d", i), uint64(i+1), base.KindPut), bytes.Repeat([]byte{'v'}, 16))
		}
		if !errors.Is(err, errBoom) {
			t.Fatalf("no write error surfaced through Add: %v", err)
		}
		if err := w.Add(ik("zzz", 99, base.KindPut), nil); !errors.Is(err, errBoom) {
			t.Fatalf("Add after I/O failure = %v, want sticky errBoom", err)
		}
		if _, err := w.Finish(); !errors.Is(err, errBoom) {
			t.Fatalf("Finish after I/O failure = %v, want sticky errBoom", err)
		}
	})
	t.Run("finish fails writing filter", func(t *testing.T) {
		w := NewWriter(&errWriter{n: 0}, WriterOptions{})
		if _, err := w.Finish(); !errors.Is(err, errBoom) {
			t.Fatalf("Finish = %v, want errBoom", err)
		}
		if _, err := w.Finish(); !errors.Is(err, errBoom) {
			t.Fatalf("second Finish = %v, want the stored I/O error, not 'already finished'", err)
		}
	})
}

func TestCallerBuffersNotRetained(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf, WriterOptions{BlockSize: 128})
	keyBuf := make([]byte, 0, 64)
	valBuf := make([]byte, 8)
	var lastKey []byte
	for i := 0; i < 100; i++ {
		keyBuf = base.AppendInternalKey(keyBuf[:0], []byte(fmt.Sprintf("key-%03d", i)), uint64(i+1), base.KindPut)
		copy(valBuf, fmt.Sprintf("val-%03d", i))
		if err := w.Add(keyBuf, valBuf); err != nil {
			t.Fatal(err)
		}
		lastKey = append(lastKey[:0], keyBuf...)
		for j := range keyBuf {
			keyBuf[j] = 0xFF
		}
		for j := range valBuf {
			valBuf[j] = 0xFF
		}
	}
	props, err := w.Finish()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(props.Largest, lastKey) {
		t.Fatalf("Largest corrupted by caller buffer reuse: %x", props.Largest)
	}
	for i, e := range parseIndex(t, buf.Bytes()) {
		checkBlock(t, buf.Bytes(), e.handle, fmt.Sprintf("data[%d]", i))
	}
}

func TestBlockCutAtExactBoundary(t *testing.T) {
	key1 := ik("aaaa", 1, base.KindPut)
	probe := newBlockBuilder(16)
	probe.add(key1, []byte("v1"))
	bs := probe.estimatedSize()

	var buf bytes.Buffer
	w := NewWriter(&buf, WriterOptions{BlockSize: bs})
	if err := w.Add(key1, []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if err := w.Add(ik("bbbb", 2, base.KindPut), []byte("v2")); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Finish(); err != nil {
		t.Fatal(err)
	}
	if n := len(parseIndex(t, buf.Bytes())); n != 2 {
		t.Fatalf("expected the >= cut to produce 2 data blocks at exact boundary, got %d", n)
	}
}

func TestFooterRejectsDamage(t *testing.T) {
	table := buildGolden(t)
	ftr := table[len(table)-footerLen:]

	for _, tc := range []struct {
		name string
		off  int
		want error
	}{
		{"magic", footerLen - 1, errBadMagic},
		{"handle bytes", 0, errBadFooter},
		{"version", 32, errBadFooter},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mut := append([]byte(nil), ftr...)
			mut[tc.off] ^= 0x01
			if _, err := decodeFooter(mut); !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
		})
	}
}
