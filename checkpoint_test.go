package basalt

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
)

func TestBatchAtomicVisibilityAndReuse(t *testing.T) {
	db, err := Open(t.TempDir(), Options{MemTableSize: 8 << 10, DisableWALSync: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	var progress atomic.Int64
	done := make(chan struct{})
	go func() {
		defer close(done)
		var b Batch
		for i := 0; i < 3000; i++ {
			b.Reset()
			b.Put([]byte(fmt.Sprintf("a-%05d", i)), value(i))
			b.Put([]byte(fmt.Sprintf("b-%05d", i)), value(i))
			if err := db.Apply(&b); err != nil {
				t.Errorf("apply %d: %v", i, err)
				return
			}
			progress.Store(int64(i + 1))
		}
	}()
	for {
		n := progress.Load()
		if n == 0 {
			continue
		}
		i := n - 1
		// If half a batch is visible, the other half must be too: the b
		// read happens after the a read, so its snapshot is at least as
		// new.
		if _, err := db.Get([]byte(fmt.Sprintf("a-%05d", i))); err == nil {
			if _, err := db.Get([]byte(fmt.Sprintf("b-%05d", i))); err != nil {
				t.Fatalf("batch %d torn: a visible, b = %v", i, err)
			}
		}
		select {
		case <-done:
			return
		default:
		}
	}
}

func TestBatchSurvivesCrashWhole(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	var b Batch
	for i := 0; i < 200; i++ {
		b.Reset()
		b.Put([]byte(fmt.Sprintf("a-%05d", i)), value(i))
		b.Delete(key(i))
		b.Put([]byte(fmt.Sprintf("b-%05d", i)), value(i))
		if err := db.Apply(&b); err != nil {
			t.Fatal(err)
		}
	}
	crash(db)

	db2, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db2.Close() }()
	for i := 0; i < 200; i++ {
		av, aerr := db2.Get([]byte(fmt.Sprintf("a-%05d", i)))
		bv, berr := db2.Get([]byte(fmt.Sprintf("b-%05d", i)))
		if aerr != nil || berr != nil || !bytes.Equal(av, value(i)) || !bytes.Equal(bv, value(i)) {
			t.Fatalf("batch %d incomplete after crash: %v %v", i, aerr, berr)
		}
		if _, err := db2.Get(key(i)); !errors.Is(err, ErrNotFound) {
			t.Fatalf("batch %d delete lost: %v", i, err)
		}
	}
}

func TestCheckpointPointInTime(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, churnOpts())
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 1000; i++ {
		mustPut(t, db, i)
	}
	for i := 100; i < 150; i++ {
		if err := db.Delete(key(i)); err != nil {
			t.Fatal(err)
		}
	}
	cpDir := filepath.Join(t.TempDir(), "cp")
	if err := db.Checkpoint(cpDir); err != nil {
		t.Fatal(err)
	}
	// Mutate the original heavily afterwards: overwrites plus enough churn
	// to compact and unlink the files the checkpoint linked.
	for i := 0; i < 1000; i++ {
		if err := db.Put(key(i), []byte("v2")); err != nil {
			t.Fatal(err)
		}
	}
	waitQuiet(t, db)

	cp, err := Open(cpDir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cp.Close() }()
	for i := 0; i < 1000; i++ {
		v, err := cp.Get(key(i))
		if i >= 100 && i < 150 {
			if !errors.Is(err, ErrNotFound) {
				t.Fatalf("checkpoint: deleted key %d = %v", i, err)
			}
			continue
		}
		if err != nil || !bytes.Equal(v, value(i)) {
			t.Fatalf("checkpoint key %d = (%q, %v), want original value", i, v, err)
		}
	}
	// And the original still sees its post-checkpoint world.
	if v, err := db.Get(key(0)); err != nil || string(v) != "v2" {
		t.Fatalf("original after checkpoint = (%q, %v)", v, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCheckpointDuringChurn(t *testing.T) {
	db, err := Open(t.TempDir(), churnOpts())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	var progress atomic.Int64
	stop := make(chan struct{})
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			if err := db.Put(key(i), value(i)); err != nil {
				t.Errorf("churn put %d: %v", i, err)
				return
			}
			progress.Store(int64(i + 1))
		}
	}()
	for progress.Load() < 500 {
	}
	ackedBefore := progress.Load()
	cpDir := filepath.Join(t.TempDir(), "cp")
	if err := db.Checkpoint(cpDir); err != nil {
		t.Fatal(err)
	}
	afterReturn := progress.Load()
	close(stop)
	<-writerDone

	cp, err := Open(cpDir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cp.Close() }()
	for i := int64(0); i < ackedBefore; i++ {
		if _, err := cp.Get(key(int(i))); err != nil {
			t.Fatalf("key %d acked before checkpoint missing: %v", i, err)
		}
	}
	if _, err := cp.Get(key(int(afterReturn) + 100)); !errors.Is(err, ErrNotFound) {
		t.Fatalf("key written after checkpoint visible: %v", err)
	}
}

func TestReplaceWithCheckpoint(t *testing.T) {
	base := t.TempDir()
	dataDir := filepath.Join(base, "db")
	db, err := Open(dataDir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		mustPut(t, db, i)
	}
	cpDir := filepath.Join(base, "cp")
	if err := db.Checkpoint(cpDir); err != nil {
		t.Fatal(err)
	}
	// Diverge, then close and roll back to the checkpoint.
	for i := 0; i < 100; i++ {
		if err := db.Put(key(i), []byte("diverged")); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := ReplaceWithCheckpoint(cpDir, dataDir); err != nil {
		t.Fatal(err)
	}
	db2, err := Open(dataDir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db2.Close() }()
	all := make([]int, 100)
	for i := range all {
		all[i] = i
	}
	verify(t, db2, all, nil)
	if _, err := os.Stat(cpDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("src must have moved away: %v", err)
	}
	if _, err := os.Stat(dataDir + ".replaced"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("old state must be removed after the swap: %v", err)
	}
}

func TestCheckpointRejectsExistingDir(t *testing.T) {
	db, err := Open(t.TempDir(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	mustPut(t, db, 1)
	if err := db.Checkpoint(t.TempDir()); err == nil {
		t.Fatal("checkpoint into an existing directory must fail")
	}
	// The engine stays healthy.
	mustPut(t, db, 2)
}
