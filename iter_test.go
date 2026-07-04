package basalt

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"testing"
)

// collectScan drains an iterator into (key, value) copies and closes it.
func collectScan(t *testing.T, it *Iterator) ([]string, []string) {
	t.Helper()
	defer it.Close()
	var keys, vals []string
	for ; it.Valid(); it.Next() {
		keys = append(keys, string(it.Key()))
		vals = append(vals, string(it.Value()))
	}
	if err := it.Error(); err != nil {
		t.Fatalf("scan: %v", err)
	}
	return keys, vals
}

// modelRange returns the model's sorted keys within [start, end).
func modelRange(model map[string]string, start, end string) ([]string, []string) {
	var keys []string
	for k := range model {
		if k >= start && (end == "" || k < end) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	vals := make([]string, len(keys))
	for i, k := range keys {
		vals[i] = model[k]
	}
	return keys, vals
}

func TestDifferentialGetScanModel(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{MemTableSize: 4 << 10}) // frequent flushes
	if err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewSource(0xba5a17))
	model := map[string]string{}
	const (
		ops      = 12_000
		keySpace = 600
	)
	kname := func(i int) string { return fmt.Sprintf("key-%04d", i) }

	checkScan := func(start, end string) {
		t.Helper()
		var s, e []byte
		if start != "" {
			s = []byte(start)
		}
		if end != "" {
			e = []byte(end)
		}
		gotK, gotV := collectScan(t, db.Scan(s, e))
		wantK, wantV := modelRange(model, start, end)
		if len(gotK) != len(wantK) {
			t.Fatalf("scan [%q,%q): %d keys, want %d", start, end, len(gotK), len(wantK))
		}
		for i := range wantK {
			if gotK[i] != wantK[i] || gotV[i] != wantV[i] {
				t.Fatalf("scan [%q,%q) at %d: (%q,%q), want (%q,%q)", start, end, i, gotK[i], gotV[i], wantK[i], wantV[i])
			}
		}
	}

	for i := 0; i < ops; i++ {
		switch r := rng.Intn(10); {
		case r < 6:
			k := kname(rng.Intn(keySpace))
			v := fmt.Sprintf("v-%d", i)
			if err := db.Put([]byte(k), []byte(v)); err != nil {
				t.Fatal(err)
			}
			model[k] = v
		case r < 8:
			k := kname(rng.Intn(keySpace))
			if err := db.Delete([]byte(k)); err != nil {
				t.Fatal(err)
			}
			delete(model, k)
		case r < 9:
			k := kname(rng.Intn(keySpace))
			v, err := db.Get([]byte(k))
			want, ok := model[k]
			if !ok {
				if !errors.Is(err, ErrNotFound) {
					t.Fatalf("op %d: get %q = %v, want ErrNotFound", i, k, err)
				}
			} else if err != nil || string(v) != want {
				t.Fatalf("op %d: get %q = (%q, %v), want %q", i, k, v, err, want)
			}
		default:
			a, b := rng.Intn(keySpace), rng.Intn(keySpace)
			if a > b {
				a, b = b, a
			}
			checkScan(kname(a), kname(b))
		}
	}
	checkScan("", "")
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db2, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	db = db2
	defer func() { _ = db2.Close() }()
	checkScan("", "")
}

func TestScanIsASnapshotAcrossFlushes(t *testing.T) {
	db, err := Open(t.TempDir(), Options{MemTableSize: 8 << 10})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	const n = 1500
	for i := 0; i < n; i++ {
		mustPut(t, db, i)
	}

	it := db.Scan(nil, nil)
	// Read a little, then force seals and flushes underneath the live
	// iterator: overwrite everything and add new keys.
	seen := 0
	for ; it.Valid() && seen < 10; it.Next() {
		seen++
	}
	for i := 0; i < n; i++ {
		if err := db.Put(key(i), []byte("AFTER")); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 500; i++ {
		if err := db.Put([]byte(fmt.Sprintf("zzz-%04d", i)), []byte("new")); err != nil {
			t.Fatal(err)
		}
	}
	// The iterator must keep yielding the snapshot: original values, no
	// zzz keys, exactly n entries.
	for ; it.Valid(); it.Next() {
		if bytes.Equal(it.Value(), []byte("AFTER")) || bytes.HasPrefix(it.Key(), []byte("zzz")) {
			t.Fatalf("snapshot leaked post-scan write: %q=%q", it.Key(), it.Value())
		}
		seen++
	}
	if err := it.Error(); err != nil {
		t.Fatal(err)
	}
	it.Close()
	if seen != n {
		t.Fatalf("snapshot scan saw %d keys, want %d", seen, n)
	}

	// A fresh scan sees the new world.
	keys, vals := collectScan(t, db.Scan(nil, nil))
	if len(keys) != n+500 {
		t.Fatalf("fresh scan saw %d keys, want %d", len(keys), n+500)
	}
	if vals[0] != "AFTER" {
		t.Fatalf("fresh scan value = %q, want AFTER", vals[0])
	}
}

func TestSnapshotIgnoresPostSnapshotTombstoneAndInsert(t *testing.T) {
	db, err := Open(t.TempDir(), Options{}) // big memtable: nothing seals
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	for i := 0; i < 100; i++ {
		mustPut(t, db, i)
	}

	it := db.Scan(nil, nil)
	for i := 0; it.Valid() && i < 5; i++ {
		it.Next()
	}
	// Ahead of the cursor: delete an existing key and insert a brand-new
	// one. Both are newer than the snapshot; the seq check must win before
	// the kind check ever sees that tombstone.
	if err := db.Delete(key(50)); err != nil {
		t.Fatal(err)
	}
	if err := db.Put([]byte("key-000050a"), []byte("intruder")); err != nil {
		t.Fatal(err)
	}
	rest := map[string]string{}
	for ; it.Valid(); it.Next() {
		rest[string(it.Key())] = string(it.Value())
	}
	if err := it.Error(); err != nil {
		t.Fatal(err)
	}
	it.Close()
	it.Close() // idempotent

	if got, ok := rest[string(key(50))]; !ok || got != string(value(50)) {
		t.Fatalf("post-snapshot tombstone leaked: key-000050 = %q, ok=%v", got, ok)
	}
	if _, ok := rest["key-000050a"]; ok {
		t.Fatal("post-snapshot insert leaked into the snapshot")
	}
	// And the live view sees the new world.
	if _, err := db.Get(key(50)); !errors.Is(err, ErrNotFound) {
		t.Fatalf("live get of deleted key = %v, want ErrNotFound", err)
	}
}

func TestScanBoundsAndTombstones(t *testing.T) {
	db, err := Open(t.TempDir(), Options{MemTableSize: 8 << 10})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	for i := 0; i < 100; i++ {
		mustPut(t, db, i)
	}
	// Delete a stripe; some deletions flush, some stay in the memtable.
	for i := 20; i < 40; i++ {
		if err := db.Delete(key(i)); err != nil {
			t.Fatal(err)
		}
	}

	keys, _ := collectScan(t, db.Scan(key(10), key(50)))
	var want []string
	for i := 10; i < 50; i++ {
		if i < 20 || i >= 40 {
			want = append(want, string(key(i)))
		}
	}
	if len(keys) != len(want) {
		t.Fatalf("bounded scan: %d keys, want %d", len(keys), len(want))
	}
	for i := range want {
		if keys[i] != want[i] {
			t.Fatalf("bounded scan at %d: %q, want %q", i, keys[i], want[i])
		}
	}

	// Degenerate ranges behave: empty, inverted, past-the-end, and a
	// non-nil empty end (the empty range, NOT an unbounded scan).
	if keys, _ := collectScan(t, db.Scan(key(60), key(60))); len(keys) != 0 {
		t.Fatalf("empty range yielded %d keys", len(keys))
	}
	if keys, _ := collectScan(t, db.Scan(key(80), key(10))); len(keys) != 0 {
		t.Fatalf("inverted range yielded %d keys", len(keys))
	}
	if keys, _ := collectScan(t, db.Scan([]byte("zzzz"), nil)); len(keys) != 0 {
		t.Fatalf("past-the-end start yielded %d keys", len(keys))
	}
	if keys, _ := collectScan(t, db.Scan(nil, []byte{})); len(keys) != 0 {
		t.Fatalf("non-nil empty end must be the empty range, yielded %d keys", len(keys))
	}
	empty, err := Open(t.TempDir(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = empty.Close() }()
	if keys, _ := collectScan(t, empty.Scan(nil, nil)); len(keys) != 0 {
		t.Fatalf("empty db yielded %d keys", len(keys))
	}
}

func TestScanAfterCloseFailsCleanly(t *testing.T) {
	db, err := Open(t.TempDir(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	mustPut(t, db, 1)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	it := db.Scan(nil, nil)
	if it.Valid() {
		t.Fatal("scan on a closed db must be invalid")
	}
	if !errors.Is(it.Error(), errClosed) {
		t.Fatalf("err = %v, want errClosed", it.Error())
	}
	it.Close() // must not panic
}
