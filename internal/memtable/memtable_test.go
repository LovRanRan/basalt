package memtable_test

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/LovRanRan/basalt/internal/base"
	"github.com/LovRanRan/basalt/internal/memtable"
)

type modelEntry struct {
	seq   uint64
	kind  base.Kind
	value string
}

// modelGet returns the newest entry for k visible at seq; entries per key
// are appended in ascending seq order.
func modelGet(model map[string][]modelEntry, k string, seq uint64) (modelEntry, bool) {
	entries := model[k]
	i := sort.Search(len(entries), func(i int) bool { return entries[i].seq > seq })
	if i == 0 {
		return modelEntry{}, false
	}
	return entries[i-1], true
}

func TestDifferential100kOps(t *testing.T) {
	rng := rand.New(rand.NewSource(0xba5a1712))
	m := memtable.New()
	model := map[string][]modelEntry{}
	var seq uint64
	const (
		ops      = 100_000
		keySpace = 512
	)
	keyName := func(i int) string { return fmt.Sprintf("key-%04d", i) }

	for i := 0; i < ops; i++ {
		switch r := rng.Intn(10); {
		case r < 6:
			seq++
			k := keyName(rng.Intn(keySpace))
			v := fmt.Sprintf("v-%d", seq)
			m.Add([]byte(k), seq, base.KindPut, []byte(v))
			model[k] = append(model[k], modelEntry{seq, base.KindPut, v})
		case r < 8:
			seq++
			k := keyName(rng.Intn(keySpace))
			m.Add([]byte(k), seq, base.KindDelete, nil)
			model[k] = append(model[k], modelEntry{seq, base.KindDelete, ""})
		default:
			k := keyName(rng.Intn(keySpace))
			var qseq uint64
			if entries := model[k]; len(entries) > 0 && rng.Intn(2) == 0 {
				// Half the queries target a version boundary exactly:
				// at e.seq the entry itself must be visible, at
				// e.seq-1 only its predecessor — this is the edge
				// that distinguishes a max-kind seek key from a
				// wrong-kind one.
				e := entries[rng.Intn(len(entries))]
				qseq = e.seq
				if rng.Intn(2) == 0 {
					qseq = e.seq - 1
				}
			} else {
				qseq = uint64(rng.Int63n(int64(seq) + 1))
			}
			gotV, gotKind, gotOK := m.Get([]byte(k), qseq)
			want, wantOK := modelGet(model, k, qseq)
			if gotOK != wantOK {
				t.Fatalf("op %d: Get(%q, %d) ok = %v, model says %v", i, k, qseq, gotOK, wantOK)
			}
			if !wantOK {
				continue
			}
			if gotKind != want.kind {
				t.Fatalf("op %d: Get(%q, %d) kind = %d, model says %d", i, k, qseq, gotKind, want.kind)
			}
			if want.kind == base.KindPut && string(gotV) != want.value {
				t.Fatalf("op %d: Get(%q, %d) = %q, model says %q", i, k, qseq, gotV, want.value)
			}
		}
	}

	type kv struct{ key, value []byte }
	var want []kv
	for k, entries := range model {
		for _, e := range entries {
			var v []byte
			if e.kind == base.KindPut {
				v = []byte(e.value)
			}
			want = append(want, kv{base.AppendInternalKey(nil, []byte(k), e.seq, e.kind), v})
		}
	}
	sort.Slice(want, func(i, j int) bool { return base.Compare(want[i].key, want[j].key) < 0 })

	it := m.NewIterator()
	i := 0
	for it.First(); it.Valid(); it.Next() {
		if i >= len(want) {
			t.Fatalf("iterator yields more than the model's %d entries", len(want))
		}
		if !bytes.Equal(it.Key(), want[i].key) {
			t.Fatalf("iteration %d: key = %x, want %x", i, it.Key(), want[i].key)
		}
		if !bytes.Equal(it.Value(), want[i].value) {
			t.Fatalf("iteration %d: value = %q, want %q", i, it.Value(), want[i].value)
		}
		i++
	}
	if i != len(want) {
		t.Fatalf("iterator yielded %d entries, model has %d", i, len(want))
	}
}

func TestIteratorBoundaries(t *testing.T) {
	m := memtable.New()
	it := m.NewIterator()
	it.First()
	if it.Valid() {
		t.Fatal("First on empty memtable must be invalid")
	}
	it.SeekGE(base.AppendSeekKey(nil, []byte("a"), base.MaxSeq))
	if it.Valid() {
		t.Fatal("SeekGE on empty memtable must be invalid")
	}

	m.Add([]byte("b"), 1, base.KindPut, []byte("vb"))
	m.Add([]byte("d"), 2, base.KindPut, []byte("vd"))
	m.Add([]byte("f"), 3, base.KindPut, []byte("vf"))
	m.Add([]byte("d"), 5, base.KindPut, []byte("vd5"))

	userKeyAt := func(it *memtable.Iterator) string {
		return string(base.UserKey(it.Key()))
	}

	it.SeekGE(base.AppendSeekKey(nil, []byte("a"), base.MaxSeq))
	if !it.Valid() || userKeyAt(it) != "b" {
		t.Fatalf("seek before all keys: got %v", it.Valid())
	}
	it.SeekGE(base.AppendSeekKey(nil, []byte("c"), base.MaxSeq))
	if !it.Valid() || userKeyAt(it) != "d" {
		t.Fatal("seek between keys must land on the next user key")
	}
	if !bytes.Equal(it.Value(), []byte("vd5")) {
		t.Fatalf("newest version of d must sort first, got %q", it.Value())
	}
	it.SeekGE(base.AppendInternalKey(nil, []byte("d"), 2, base.KindPut))
	if !it.Valid() || !bytes.Equal(it.Value(), []byte("vd")) {
		t.Fatal("exact internal-key seek must land on that version")
	}
	it.SeekGE(base.AppendSeekKey(nil, []byte("d"), 4))
	if !it.Valid() || !bytes.Equal(it.Value(), []byte("vd")) {
		t.Fatal("seek at snapshot seq 4 must skip the seq-5 version of d")
	}
	it.SeekGE(base.AppendSeekKey(nil, []byte("g"), base.MaxSeq))
	if it.Valid() {
		t.Fatal("seek past all keys must be invalid")
	}

	var got []string
	for it.First(); it.Valid(); it.Next() {
		got = append(got, userKeyAt(it))
	}
	want := []string{"b", "d", "d", "f"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("full iteration = %v, want %v", got, want)
	}
}

func TestConcurrentReadersDuringWrites(t *testing.T) {
	m := memtable.New()
	const n = 30_000
	var watermark atomic.Uint64
	done := make(chan struct{})

	// Insert keys in a random order so splices land at arbitrary skiplist
	// positions that reader seeks actually traverse — ascending inserts
	// would only ever splice at the tail, past every key being read.
	perm := rand.New(rand.NewSource(42)).Perm(n)
	key := func(id int) []byte { return []byte(fmt.Sprintf("k%06d", id)) }

	go func() {
		defer close(done)
		for i := 0; i < n; i++ {
			id := perm[i]
			m.Add(key(id), uint64(i+1), base.KindPut, []byte(fmt.Sprintf("v%06d", id)))
			watermark.Store(uint64(i + 1))
		}
	}()

	var wg sync.WaitGroup
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(seed)))
			for {
				if w := watermark.Load(); w > 0 {
					id := perm[rng.Int63n(int64(w))]
					v, kind, ok := m.Get(key(id), base.MaxSeq)
					if !ok || kind != base.KindPut || string(v) != fmt.Sprintf("v%06d", id) {
						t.Errorf("acknowledged key k%06d not readable: ok=%v kind=%d v=%q", id, ok, kind, v)
						return
					}
				}
				if rng.Intn(64) == 0 {
					it := m.NewIterator()
					it.SeekGE(base.AppendSeekKey(nil, key(rng.Intn(n)), base.MaxSeq))
					var prev []byte
					count := 0
					for ; it.Valid() && count < 2000; it.Next() {
						if prev != nil && base.Compare(prev, it.Key()) >= 0 {
							t.Error("iterator order violation during concurrent writes")
							return
						}
						prev = append(prev[:0], it.Key()...)
						count++
					}
				}
				select {
				case <-done:
					return
				default:
				}
			}
		}(r)
	}
	<-done
	wg.Wait()

	for i := 0; i < n; i += 997 {
		if _, _, ok := m.Get(key(i), base.MaxSeq); !ok {
			t.Fatalf("key k%06d missing after writer finished", i)
		}
	}
}

func TestNextOnInvalidIteratorPanics(t *testing.T) {
	m := memtable.New()
	m.Add([]byte("a"), 1, base.KindPut, nil)
	it := m.NewIterator()
	it.First()
	it.Next()
	if it.Valid() {
		t.Fatal("iterator should be exhausted")
	}
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic from Next on an invalid iterator")
		}
	}()
	it.Next()
}

func TestApproximateSizeGrowsMonotonically(t *testing.T) {
	m := memtable.New()
	prev := m.ApproximateSize()
	if prev != 0 {
		t.Fatalf("empty memtable size = %d, want 0", prev)
	}
	var payload int64
	for i := 0; i < 1000; i++ {
		k := []byte(fmt.Sprintf("key-%d", i))
		v := bytes.Repeat([]byte{'x'}, i%100)
		m.Add(k, uint64(i+1), base.KindPut, v)
		payload += int64(len(k)) + base.TrailerLen + int64(len(v))
		s := m.ApproximateSize()
		if s <= prev {
			t.Fatalf("size not strictly monotonic at insert %d: %d -> %d", i, prev, s)
		}
		prev = s
	}
	if prev < payload {
		t.Fatalf("approximate size %d below raw payload %d", prev, payload)
	}
}

func TestDuplicateInternalKeyPanics(t *testing.T) {
	m := memtable.New()
	m.Add([]byte("k"), 5, base.KindPut, []byte("v"))
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on duplicate internal key")
		}
	}()
	m.Add([]byte("k"), 5, base.KindPut, []byte("v2"))
}

func TestValueIsCopiedOnAdd(t *testing.T) {
	m := memtable.New()
	v := []byte("original")
	m.Add([]byte("k"), 1, base.KindPut, v)
	v[0] = 'X'
	got, _, ok := m.Get([]byte("k"), base.MaxSeq)
	if !ok || string(got) != "original" {
		t.Fatalf("stored value affected by caller mutation: %q", got)
	}
}
