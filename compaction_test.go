package basalt

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/LovRanRan/basalt/internal/base"
	"github.com/LovRanRan/basalt/internal/manifest"
)

// churnOpts force constant flush and compaction cascades.
func churnOpts() Options {
	return Options{
		MemTableSize:       16 << 10,
		L0CompactThreshold: 2,
		L0StopThreshold:    8,
		LevelBytesBase:     64 << 10,
		TargetFileSize:     32 << 10,
		DisableWALSync:     true,
	}
}

// waitQuiet blocks until no background work is running.
func waitQuiet(t *testing.T, db *DB) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		db.mu.Lock()
		quiet := db.flushers == 0 && !db.compacting && db.rs.imm == nil && db.bgErr == nil
		err := db.bgErr
		db.mu.Unlock()
		if err != nil {
			t.Fatalf("background error: %v", err)
		}
		if quiet {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("background work never quiesced")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// checkLevelInvariants asserts deeper levels are sorted and disjoint.
func checkLevelInvariants(t *testing.T, db *DB) {
	t.Helper()
	v := db.vs.Current()
	for lvl := 1; lvl < manifest.NumLevels; lvl++ {
		files := v.Files[lvl]
		for i := 0; i+1 < len(files); i++ {
			if base.Compare(files[i].Largest, files[i+1].Smallest) >= 0 {
				t.Fatalf("level %d files %06d/%06d overlap", lvl, files[i].FileNum, files[i+1].FileNum)
			}
		}
	}
}

func TestCompactionChurn200k(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, churnOpts())
	if err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewSource(0xba5a17))
	model := map[string]string{}
	const (
		ops      = 200_000
		keySpace = 3000
	)
	kname := func(i int) string { return fmt.Sprintf("key-%05d", i) }

	fullCompare := func() {
		t.Helper()
		it := db.Scan(nil, nil)
		got := map[string]string{}
		for ; it.Valid(); it.Next() {
			got[string(it.Key())] = string(it.Value())
		}
		if err := it.Error(); err != nil {
			t.Fatal(err)
		}
		it.Close()
		if len(got) != len(model) {
			t.Fatalf("scan has %d keys, model has %d", len(got), len(model))
		}
		for k, v := range model {
			if got[k] != v {
				t.Fatalf("key %q = %q, model %q", k, got[k], v)
			}
		}
	}

	for i := 0; i < ops; i++ {
		k := kname(rng.Intn(keySpace))
		if rng.Intn(10) < 7 {
			v := fmt.Sprintf("v-%d-%s", i, string(bytes.Repeat([]byte{'x'}, rng.Intn(150))))
			if err := db.Put([]byte(k), []byte(v)); err != nil {
				t.Fatalf("op %d: %v", i, err)
			}
			model[k] = v
		} else {
			if err := db.Delete([]byte(k)); err != nil {
				t.Fatalf("op %d: %v", i, err)
			}
			delete(model, k)
		}
		if i%25_000 == 24_999 {
			fullCompare()
			checkLevelInvariants(t, db)
		}
	}
	waitQuiet(t, db)
	fullCompare()
	checkLevelInvariants(t, db)

	// Compactions actually happened: deeper levels are populated.
	deep := false
	for lvl := 1; lvl < manifest.NumLevels; lvl++ {
		if len(db.vs.Current().Files[lvl]) > 0 {
			deep = true
		}
	}
	if !deep {
		t.Fatal("no compaction ever pushed data below L0")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db2, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db2.Close() }()
	db = db2
	fullCompare()
	checkLevelInvariants(t, db2)
}

func TestCrashMidCompactionBeforeApply(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{
		MemTableSize:       8 << 10,
		L0CompactThreshold: 2,
		LevelBytesBase:     32 << 10,
		TargetFileSize:     16 << 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	db.hookBeforeCompactApply = func() error { return errBoom }
	acked := fillUntilPoisoned(t, db, 8000)
	if len(acked) == 0 {
		t.Fatal("nothing acked before the crash")
	}
	crash(db)

	db2, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db2.Close() }()
	verify(t, db2, acked, nil)
	// Orphan compaction outputs were collected.
	live := db2.vs.Current().Live()
	for num := range db2.tables {
		if !live[num] {
			t.Fatalf("table %06d open but not live", num)
		}
	}
}

func TestCrashMidCompactionAfterApply(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{
		MemTableSize:       8 << 10,
		L0CompactThreshold: 2,
		LevelBytesBase:     32 << 10,
		TargetFileSize:     16 << 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	db.hookAfterCompactApply = func() error { return errBoom }
	acked := fillUntilPoisoned(t, db, 8000)
	if len(acked) == 0 {
		t.Fatal("nothing acked before the crash")
	}
	crash(db)

	// The compaction's edit committed but its inputs were never unlinked:
	// reopen collects them and every acked write survives, exactly once.
	db2, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db2.Close() }()
	verify(t, db2, acked, nil)
	checkLevelInvariants(t, db2)
}

func TestTombstoneGCAtBottomLevel(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, churnOpts())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	// Write a doomed key range and push it below L0.
	for i := 0; i < 400; i++ {
		if err := db.Put([]byte(fmt.Sprintf("doomed-%04d", i)), bytes.Repeat([]byte{'d'}, 100)); err != nil {
			t.Fatal(err)
		}
	}
	waitQuiet(t, db)
	// Delete all of it, then churn unrelated keys so the tombstones flush
	// and compact through.
	for i := 0; i < 400; i++ {
		if err := db.Delete([]byte(fmt.Sprintf("doomed-%04d", i))); err != nil {
			t.Fatal(err)
		}
	}
	for round := 0; round < 40; round++ {
		for i := 0; i < 200; i++ {
			if err := db.Put([]byte(fmt.Sprintf("filler-%02d-%04d", round, i)), bytes.Repeat([]byte{'f'}, 120)); err != nil {
				t.Fatal(err)
			}
		}
		waitQuiet(t, db)
	}

	// The doomed range must be gone from the user's view...
	it := db.Scan([]byte("doomed-"), []byte("doomed-~"))
	if it.Valid() {
		t.Fatalf("deleted range still visible: %q", it.Key())
	}
	it.Close()

	// ...and physically gone from every table: neither values nor
	// tombstones survive once nothing deeper could resurrect them.
	rs, _, err := db.acquire()
	if err != nil {
		t.Fatal(err)
	}
	defer rs.release()
	found := 0
	for _, level := range rs.levels {
		for _, h := range level {
			ti := h.r.NewIterator()
			for ti.SeekGE(base.AppendSeekKey(nil, []byte("doomed-"), base.MaxSeq)); ti.Valid(); ti.Next() {
				if !bytes.HasPrefix(base.UserKey(ti.Key()), []byte("doomed-")) {
					break
				}
				found++
			}
			if err := ti.Error(); err != nil {
				t.Fatal(err)
			}
		}
	}
	if found != 0 {
		t.Fatalf("%d doomed entries (values or tombstones) survive in tables", found)
	}
}

func TestIteratorSurvivesCompactionRetiringItsFiles(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, churnOpts())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	const n = 1200
	for i := 0; i < n; i++ {
		mustPut(t, db, i)
	}
	waitQuiet(t, db)

	// Pin an iterator, then churn hard enough that compaction retires and
	// unlinks the very files it is reading.
	it := db.Scan(nil, nil)
	seen := 0
	for ; it.Valid() && seen < 20; it.Next() {
		seen++
	}
	for i := 0; i < n; i++ {
		if err := db.Put(key(i), []byte("NEW")); err != nil {
			t.Fatal(err)
		}
	}
	waitQuiet(t, db)

	for ; it.Valid(); it.Next() {
		if bytes.Equal(it.Value(), []byte("NEW")) {
			t.Fatal("pinned iterator saw a post-snapshot value")
		}
		seen++
	}
	if err := it.Error(); err != nil {
		t.Fatalf("pinned iterator failed after compaction churn: %v", err)
	}
	it.Close()
	if seen != n {
		t.Fatalf("pinned iterator saw %d keys, want %d", seen, n)
	}
	if !errors.Is(func() error { _, err := db.Get([]byte("nope")); return err }(), ErrNotFound) {
		t.Fatal("sanity get failed")
	}
}
