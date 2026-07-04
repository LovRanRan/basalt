package sstable

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"testing"

	"github.com/LovRanRan/basalt/internal/base"
)

type tEntry struct {
	key   []byte
	value []byte
}

// buildRandomTable writes n entries — multiple versions per user key,
// tombstones, empty and random values — and returns the table bytes plus
// the entries in written order.
func buildRandomTable(t *testing.T, n int, opts WriterOptions) ([]byte, []tEntry) {
	t.Helper()
	rng := rand.New(rand.NewSource(int64(n)))
	var entries []tEntry
	seq := uint64(0)
	userKeys := 0
	for len(entries) < n {
		uk := fmt.Sprintf("key-%08d", userKeys)
		userKeys++
		versions := 1 + rng.Intn(3)
		seqs := make([]uint64, versions)
		for i := range seqs {
			seq += uint64(1 + rng.Intn(3))
			seqs[i] = seq
		}
		for i := versions - 1; i >= 0 && len(entries) < n; i-- {
			kind := base.KindPut
			var val []byte
			if rng.Intn(5) == 0 {
				kind = base.KindDelete
			} else if rng.Intn(8) != 0 {
				val = make([]byte, rng.Intn(60))
				rng.Read(val)
			}
			entries = append(entries, tEntry{base.AppendInternalKey(nil, []byte(uk), seqs[i], kind), val})
		}
	}
	var buf bytes.Buffer
	w := NewWriter(&buf, opts)
	for _, e := range entries {
		if err := w.Add(e.key, e.value); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := w.Finish(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes(), entries
}

func TestRoundtrip100k(t *testing.T) {
	table, entries := buildRandomTable(t, 100_000, WriterOptions{})
	r, err := NewReader(bytes.NewReader(table), int64(len(table)))
	if err != nil {
		t.Fatal(err)
	}

	it := r.NewIterator()
	i := 0
	for it.First(); it.Valid(); it.Next() {
		if i >= len(entries) {
			t.Fatal("iterator yields more entries than written")
		}
		if !bytes.Equal(it.Key(), entries[i].key) {
			t.Fatalf("entry %d: key mismatch", i)
		}
		if !bytes.Equal(it.Value(), entries[i].value) {
			t.Fatalf("entry %d: value mismatch", i)
		}
		i++
	}
	if err := it.Error(); err != nil {
		t.Fatal(err)
	}
	if i != len(entries) {
		t.Fatalf("iterated %d entries, want %d", i, len(entries))
	}

	rng := rand.New(rand.NewSource(7))
	for trial := 0; trial < 20_000; trial++ {
		ei := rng.Intn(len(entries))
		ik, err := base.DecodeInternalKey(entries[ei].key)
		if err != nil {
			t.Fatal(err)
		}
		v, kind, ok, err := r.Get(ik.UserKey, ik.Seq)
		if err != nil {
			t.Fatal(err)
		}
		if !ok || kind != ik.Kind || !bytes.Equal(v, entries[ei].value) {
			t.Fatalf("Get(%q, %d) = (%q, %d, %v), want exact version %d", ik.UserKey, ik.Seq, v, kind, ok, ei)
		}
	}
	for i := 0; i < 10_000; i++ {
		if _, _, ok, err := r.Get([]byte(fmt.Sprintf("absent-%06d", i)), base.MaxSeq); err != nil || ok {
			t.Fatalf("absent key: ok=%v err=%v", ok, err)
		}
	}
	for trial := 0; trial < 10_000; trial++ {
		var target []byte
		if rng.Intn(2) == 0 {
			target = entries[rng.Intn(len(entries))].key
		} else {
			target = base.AppendSeekKey(nil, []byte(fmt.Sprintf("key-%08d", rng.Intn(45_000))), uint64(rng.Intn(300_000)))
		}
		want := sort.Search(len(entries), func(i int) bool { return base.Compare(entries[i].key, target) >= 0 })
		sit := r.NewIterator()
		sit.SeekGE(target)
		if want == len(entries) {
			if sit.Valid() {
				t.Fatal("SeekGE past the table must be invalid")
			}
			if err := sit.Error(); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if !sit.Valid() {
			t.Fatalf("SeekGE(%x): invalid, want entry %d (err=%v)", target, want, sit.Error())
		}
		if !bytes.Equal(sit.Key(), entries[want].key) {
			t.Fatalf("SeekGE(%x) landed on wrong entry", target)
		}
	}
}

func TestEveryByteFlipDetected(t *testing.T) {
	table, entries := buildRandomTable(t, 40, WriterOptions{BlockSize: 128, RestartInterval: 3})
	readAll := func(tbl []byte) error {
		r, err := NewReader(bytes.NewReader(tbl), int64(len(tbl)))
		if err != nil {
			return err
		}
		it := r.NewIterator()
		n := 0
		for it.First(); it.Valid(); it.Next() {
			n++
		}
		if err := it.Error(); err != nil {
			return err
		}
		if n != len(entries) {
			return fmt.Errorf("iterated %d, want %d", n, len(entries))
		}
		for i := 0; i < len(entries); i += 7 {
			ik, err := base.DecodeInternalKey(entries[i].key)
			if err != nil {
				return err
			}
			v, kind, ok, err := r.Get(ik.UserKey, ik.Seq)
			if err != nil {
				return err
			}
			if !ok || kind != ik.Kind || !bytes.Equal(v, entries[i].value) {
				return fmt.Errorf("get mismatch at %d", i)
			}
		}
		return nil
	}
	if err := readAll(table); err != nil {
		t.Fatalf("pristine table: %v", err)
	}
	for off := 0; off < len(table); off++ {
		mut := append([]byte(nil), table...)
		mut[off] ^= 0x01
		if err := readAll(mut); err == nil {
			t.Fatalf("bit flip at offset %d of %d went undetected", off, len(table))
		}
	}
}

func TestReaderOnGolden(t *testing.T) {
	table := buildGolden(t)
	r, err := NewReader(bytes.NewReader(table), int64(len(table)))
	if err != nil {
		t.Fatal(err)
	}

	it := r.NewIterator()
	it.SeekGE(base.AppendSeekKey(nil, []byte("a"), base.MaxSeq))
	if !it.Valid() || !bytes.Equal(it.Key(), ik("user:0001", 9, base.KindPut)) || string(it.Value()) != "alice" {
		t.Fatalf("seek before all keys: valid=%v err=%v", it.Valid(), it.Error())
	}

	count := 0
	for it.First(); it.Valid(); it.Next() {
		count++
	}
	if err := it.Error(); err != nil || count != 9 {
		t.Fatalf("full iteration: %d entries, err=%v", count, err)
	}

	it.SeekGE(ik("user:0002", 3, base.KindPut))
	if !it.Valid() || string(it.Value()) != "bob" {
		t.Fatal("exact internal-key seek must land on that version")
	}
	it.SeekGE(base.AppendSeekKey(nil, []byte("zzzz"), base.MaxSeq))
	if it.Valid() || it.Error() != nil {
		t.Fatal("seek past the table must be invalid without error")
	}

	// Version straddle: user:0002 versions span a block boundary.
	v, kind, ok, err := r.Get([]byte("user:0002"), 2)
	if err != nil || !ok || kind != base.KindPut || string(v) != "b2" {
		t.Fatalf("Get(user:0002, 2) = (%q, %d, %v, %v), want b2", v, kind, ok, err)
	}
	if _, kind, ok, err = r.Get([]byte("user:0002"), base.MaxSeq); err != nil || !ok || kind != base.KindDelete {
		t.Fatalf("newest user:0002 must be the tombstone: kind=%d ok=%v err=%v", kind, ok, err)
	}
	if _, _, ok, err = r.Get([]byte("user:0005"), base.MaxSeq); err != nil || ok {
		t.Fatalf("absent key between blocks: ok=%v err=%v", ok, err)
	}
	if _, _, ok, err = r.Get([]byte("zzz"), 0); err != nil || ok {
		t.Fatalf("query below the oldest version: ok=%v err=%v", ok, err)
	}
	if v, _, ok, _ := r.Get([]byte("zzz"), base.MaxSeq); !ok || string(v) != "end" {
		t.Fatal("largest key must be readable")
	}

	defer func() {
		if recover() == nil {
			t.Fatal("Next on invalid iterator must panic")
		}
	}()
	it.Next()
}

func TestReaderEmptyTable(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf, WriterOptions{})
	if _, err := w.Finish(); err != nil {
		t.Fatal(err)
	}
	r, err := NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, ok, err := r.Get([]byte("any"), base.MaxSeq); err != nil || ok {
		t.Fatalf("empty table get: ok=%v err=%v", ok, err)
	}
	it := r.NewIterator()
	it.First()
	if it.Valid() || it.Error() != nil {
		t.Fatal("empty table First must be invalid without error")
	}
	it.SeekGE(base.AppendSeekKey(nil, []byte("any"), base.MaxSeq))
	if it.Valid() || it.Error() != nil {
		t.Fatal("empty table SeekGE must be invalid without error")
	}
}

func TestConcurrentReaders(t *testing.T) {
	table, entries := buildRandomTable(t, 5_000, WriterOptions{})
	r, err := NewReader(bytes.NewReader(table), int64(len(table)))
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			for i := 0; i < 2_000; i++ {
				e := entries[rng.Intn(len(entries))]
				dk, err := base.DecodeInternalKey(e.key)
				if err != nil {
					t.Error(err)
					return
				}
				v, kind, ok, err := r.Get(dk.UserKey, dk.Seq)
				if err != nil || !ok || kind != dk.Kind || !bytes.Equal(v, e.value) {
					t.Errorf("concurrent Get mismatch: ok=%v err=%v", ok, err)
					return
				}
			}
			it := r.NewIterator()
			n := 0
			for it.First(); it.Valid() && n < 3_000; it.Next() {
				n++
			}
			if it.Error() != nil {
				t.Errorf("concurrent iteration: %v", it.Error())
			}
		}(int64(g))
	}
	wg.Wait()
}

func TestGetValueIsCallerOwned(t *testing.T) {
	table, entries := buildRandomTable(t, 100, WriterOptions{BlockSize: 256})
	r, err := NewReader(bytes.NewReader(table), int64(len(table)))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if len(e.value) == 0 {
			continue
		}
		dk, err := base.DecodeInternalKey(e.key)
		if err != nil {
			t.Fatal(err)
		}
		v1, _, ok, err := r.Get(dk.UserKey, dk.Seq)
		if err != nil || !ok {
			t.Fatal(ok, err)
		}
		for i := range v1 {
			v1[i] = 0xFF
		}
		v2, _, _, err := r.Get(dk.UserKey, dk.Seq)
		if err != nil || !bytes.Equal(v2, e.value) {
			t.Fatalf("scribbling a returned value must not affect later reads: %v", err)
		}
		break
	}
}

func TestIteratorCopiesSurviveNext(t *testing.T) {
	table, entries := buildRandomTable(t, 50, WriterOptions{BlockSize: 128})
	r, err := NewReader(bytes.NewReader(table), int64(len(table)))
	if err != nil {
		t.Fatal(err)
	}
	it := r.NewIterator()
	it.First()
	for i := 0; i < 3; i++ {
		it.Next()
	}
	keyCopy := append([]byte(nil), it.Key()...)
	valCopy := append([]byte(nil), it.Value()...)
	for i := 0; i < 5 && it.Valid(); i++ {
		it.Next()
	}
	if !bytes.Equal(keyCopy, entries[3].key) || !bytes.Equal(valCopy, entries[3].value) {
		t.Fatal("copies taken before Next must remain intact")
	}
}

func TestNewReaderRejectsGarbage(t *testing.T) {
	if _, err := NewReader(bytes.NewReader(nil), 0); !errors.Is(err, ErrCorruption) {
		t.Fatalf("zero-byte file: %v", err)
	}
	junk := bytes.Repeat([]byte{0x42}, 100)
	if _, err := NewReader(bytes.NewReader(junk), int64(len(junk))); !errors.Is(err, ErrCorruption) {
		t.Fatalf("junk bytes: %v", err)
	}
	table := buildGolden(t)
	half := table[:len(table)/2]
	if _, err := NewReader(bytes.NewReader(half), int64(len(half))); !errors.Is(err, ErrCorruption) {
		t.Fatalf("truncated table: %v", err)
	}
}
