package basalt

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

var errBoom = errors.New("boom")

// crash abandons a DB the way a process death would: the flock is released
// (a dead process cannot hold one) while every other fd and all unflushed
// state are simply dropped.
func crash(db *DB) {
	_ = db.lock.Close()
}

func key(i int) []byte   { return []byte(fmt.Sprintf("key-%06d", i)) }
func value(i int) []byte { return []byte(fmt.Sprintf("value-%06d", i)) }

func mustPut(t *testing.T, db *DB, i int) {
	t.Helper()
	if err := db.Put(key(i), value(i)); err != nil {
		t.Fatalf("put %d: %v", i, err)
	}
}

// verify checks that keys in alive are present with their values and keys
// in dead return ErrNotFound.
func verify(t *testing.T, db *DB, alive, dead []int) {
	t.Helper()
	for _, i := range alive {
		v, err := db.Get(key(i))
		if err != nil {
			t.Fatalf("get %d: %v", i, err)
		}
		if !bytes.Equal(v, value(i)) {
			t.Fatalf("get %d = %q, want %q", i, v, value(i))
		}
	}
	for _, i := range dead {
		if _, err := db.Get(key(i)); !errors.Is(err, ErrNotFound) {
			t.Fatalf("deleted key %d: err = %v, want ErrNotFound", i, err)
		}
	}
}

func TestBasicPutGetDeleteAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	var alive, dead []int
	for i := 0; i < 200; i++ {
		mustPut(t, db, i)
	}
	// Overwrite a few, delete a few.
	if err := db.Put(key(7), []byte("overwritten")); err != nil {
		t.Fatal(err)
	}
	for i := 100; i < 120; i++ {
		if err := db.Delete(key(i)); err != nil {
			t.Fatal(err)
		}
		dead = append(dead, i)
	}
	for i := 0; i < 100; i++ {
		if i != 7 {
			alive = append(alive, i)
		}
	}
	for i := 120; i < 200; i++ {
		alive = append(alive, i)
	}
	verify(t, db, alive, dead)
	if v, err := db.Get(key(7)); err != nil || string(v) != "overwritten" {
		t.Fatalf("overwritten key = %q, %v", v, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db2, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db2.Close() }()
	verify(t, db2, alive, dead)
	if v, err := db2.Get(key(7)); err != nil || string(v) != "overwritten" {
		t.Fatalf("overwritten key after reopen = %q, %v", v, err)
	}
}

func TestCrashAfterWALAppend(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	var alive, dead []int
	for i := 0; i < 300; i++ {
		mustPut(t, db, i)
		alive = append(alive, i)
	}
	for i := 0; i < 300; i += 10 {
		if err := db.Delete(key(i)); err != nil {
			t.Fatal(err)
		}
		dead = append(dead, i)
		alive = append(alive[:i/10*9], alive[i/10*9+1:]...)
	}
	// Crash: abandon without Close — nothing flushed, only the WAL holds
	// the data.
	crash(db)
	db2, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db2.Close() }()
	verify(t, db2, alive, dead)
}

// Crash model for these tests: a crash is in-process abandonment — file
// descriptors stay open, every completed write is intact (torn frames are
// the wal package's tested territory), and unix semantics let the new Open
// truncate or delete files the zombie still holds. A subprocess-SIGKILL
// harness is P4.9's job.
//
// fillUntilPoisoned puts keys 0..max-1, stopping at the first failure —
// the simulated crash point. Only acknowledged writes count; the failure
// must trace back to the injected errBoom.
func fillUntilPoisoned(t *testing.T, db *DB, max int) []int {
	t.Helper()
	var acked []int
	for i := 0; i < max; i++ {
		if err := db.Put(key(i), value(i)); err != nil {
			if !errors.Is(err, errBoom) {
				t.Fatalf("put %d failed with %v, want the injected crash", i, err)
			}
			return acked
		}
		acked = append(acked, i)
	}
	t.Fatal("fill never hit the injected crash — raise the write count")
	return nil
}

func TestCrashAfterTableWriteBeforeApply(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{MemTableSize: 8 << 10})
	if err != nil {
		t.Fatal(err)
	}
	db.hookAfterTableWrite = func() error { return errBoom }
	acked := fillUntilPoisoned(t, db, 4000)
	if len(acked) == 0 {
		t.Fatal("no writes acked before the crash")
	}
	// The flush died after writing the orphan table but before the
	// manifest edit; every acked write is still in the WAL.
	crash(db)
	db2, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db2.Close() }()
	verify(t, db2, acked, nil)
	// The orphan was collected: every table on disk is live.
	live := db2.vs.Current().Live()
	tables, err := filepath.Glob(filepath.Join(dir, "*.sst"))
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range tables {
		var num uint64
		if _, err := fmt.Sscanf(filepath.Base(p), "%06d.sst", &num); err != nil {
			t.Fatal(err)
		}
		if !live[num] {
			t.Fatalf("orphan table %s survived reopen", p)
		}
	}
}

func TestCrashAfterApplyBeforeWALPrune(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{MemTableSize: 8 << 10})
	if err != nil {
		t.Fatal(err)
	}
	db.hookAfterApply = func() error { return errBoom }
	var alive, dead []int
	for i := 0; i < 4000; i++ {
		if err := db.Put(key(i), value(i)); err != nil {
			if !errors.Is(err, errBoom) {
				t.Fatalf("put %d failed with %v, want the injected crash", i, err)
			}
			break
		}
		if i%25 == 0 {
			if err := db.Delete(key(i)); err != nil {
				if !errors.Is(err, errBoom) {
					t.Fatalf("delete %d failed with %v", i, err)
				}
				break
			}
			dead = append(dead, i)
			continue
		}
		alive = append(alive, i)
	}
	if len(dead) == 0 || len(alive) == 0 {
		t.Fatal("crash happened before any interesting state accumulated")
	}
	// The flush committed its manifest edit but never pruned the WAL:
	// reopen must not re-apply flushed batches (duplicate seqs panic the
	// memtable) and deleted keys must stay dead — no phantoms.
	crash(db)
	db2, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db2.Close() }()
	verify(t, db2, alive, dead)
}

func TestFlushesProduceTablesAndSurviveReopen(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{MemTableSize: 8 << 10})
	if err != nil {
		t.Fatal(err)
	}
	const n = 2000
	for i := 0; i < n; i++ {
		mustPut(t, db, i)
	}
	// Overwrites spanning seals: newest version must win everywhere.
	for i := 0; i < n; i += 100 {
		if err := db.Put(key(i), []byte("v2")); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db2, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db2.Close() }()
	if len(db2.l0) < 2 {
		t.Fatalf("expected multiple L0 tables, got %d", len(db2.l0))
	}
	for i := 0; i < n; i++ {
		v, err := db2.Get(key(i))
		if err != nil {
			t.Fatalf("get %d: %v", i, err)
		}
		want := value(i)
		if i%100 == 0 {
			want = []byte("v2")
		}
		if !bytes.Equal(v, want) {
			t.Fatalf("get %d = %q, want %q", i, v, want)
		}
	}
}

func TestConcurrentWritersAndReaders(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{MemTableSize: 4 << 10})
	if err != nil {
		t.Fatal(err)
	}
	const (
		writers      = 4
		perWriter    = 400
		readersCount = 4
	)
	var acked [writers]atomic.Int64
	var wg, writersWg sync.WaitGroup
	done := make(chan struct{})

	for w := 0; w < writers; w++ {
		wg.Add(1)
		writersWg.Add(1)
		go func(w int) {
			defer wg.Done()
			defer writersWg.Done()
			for i := 0; i < perWriter; i++ {
				id := w*perWriter + i
				if err := db.Put(key(id), value(id)); err != nil {
					t.Errorf("writer %d: %v", w, err)
					return
				}
				acked[w].Store(int64(i + 1))
			}
		}(w)
	}
	for r := 0; r < readersCount; r++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			for {
				select {
				case <-done:
					return
				default:
				}
				w := rng.Intn(writers)
				n := acked[w].Load()
				if n == 0 {
					runtime.Gosched()
					continue
				}
				id := w*perWriter + rng.Intn(int(n))
				v, err := db.Get(key(id))
				if err != nil || !bytes.Equal(v, value(id)) {
					t.Errorf("acked key %d: %q, %v", id, v, err)
					return
				}
			}
		}(int64(r))
	}

	go func() {
		writersWg.Wait()
		close(done)
	}()
	wg.Wait()

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db2, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db2.Close() }()
	for id := 0; id < writers*perWriter; id++ {
		v, err := db2.Get(key(id))
		if err != nil || !bytes.Equal(v, value(id)) {
			t.Fatalf("key %d after reopen: %q, %v", id, v, err)
		}
	}
}

func TestGetOnEmptyDB(t *testing.T) {
	db, err := Open(t.TempDir(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.Get([]byte("nothing")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

func TestOversizedWriteDoesNotPoison(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	huge := make([]byte, 65<<20) // over the WAL's 64 MiB record limit
	if err := db.Put([]byte("huge"), huge); !errors.Is(err, ErrBatchTooLarge) {
		t.Fatalf("oversized put = %v, want ErrBatchTooLarge", err)
	}
	mustPut(t, db, 1)
	if v, err := db.Get(key(1)); err != nil || !bytes.Equal(v, value(1)) {
		t.Fatalf("engine unusable after oversized write: %q, %v", v, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db2, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db2.Close() }()
	verify(t, db2, []int{1}, nil)
	if _, err := db2.Get([]byte("huge")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("rejected write must not resurface: %v", err)
	}
}

func TestReopenChain(t *testing.T) {
	dir := t.TempDir()
	var alive, dead []int
	for gen := 0; gen < 3; gen++ {
		db, err := Open(dir, Options{MemTableSize: 8 << 10})
		if err != nil {
			t.Fatalf("generation %d: %v", gen, err)
		}
		lo, hi := gen*300, (gen+1)*300
		for i := lo; i < hi; i++ {
			mustPut(t, db, i)
			if i%50 == 0 {
				if err := db.Delete(key(i)); err != nil {
					t.Fatal(err)
				}
				dead = append(dead, i)
			} else {
				alive = append(alive, i)
			}
		}
		verify(t, db, alive, dead)
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	verify(t, db, alive, dead)
}

func TestSecondOpenIsLocked(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir, Options{}); err == nil {
		t.Fatal("second Open on a live directory must fail")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db2, err := Open(dir, Options{})
	if err != nil {
		t.Fatalf("open after close: %v", err)
	}
	_ = db2.Close()
}

func TestCloseIsIdempotentAndCleans(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		mustPut(t, db, i)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
	if err := db.Put(key(1), nil); err == nil {
		t.Fatal("put after close must fail")
	}
	if _, err := db.Get(key(1)); err == nil {
		t.Fatal("get after close must fail")
	}
	// Clean close flushed everything: reopen sees the data with no WAL
	// replay needed (the wal dir holds only fresh empty segments).
	db2, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db2.Close() }()
	all := make([]int, 50)
	for i := range all {
		all[i] = i
	}
	verify(t, db2, all, nil)
}
