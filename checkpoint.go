package basalt

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/LovRanRan/basalt/internal/manifest"
)

// Checkpoint writes a consistent point-in-time copy of the database into
// dst, which must not exist yet and must live on the same filesystem as the
// database (tables are hard-linked, not copied — so checkpoints are cheap
// and immutable: table files are never modified in place, and compaction
// unlinking the original names cannot disturb the links). The memtable is
// flushed first, so the checkpoint is entirely tables plus a fresh manifest,
// with no WAL.
//
// The checkpoint is itself a complete database: Open it directly for a
// stable snapshot iterator, or install it with ReplaceWithCheckpoint.
// Checkpoint blocks writes for its duration; background work is drained
// first. A failed Checkpoint leaves no dst behind.
func (db *DB) Checkpoint(dst string) (err error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Drain in-flight background work, then flush the current memtable
	// once so everything acknowledged before this call is on disk.
	for db.flushers > 0 || db.compacting || db.rs.imm != nil {
		if db.closed || db.closing {
			return ErrClosed
		}
		if db.bgErr != nil {
			return db.bgErr
		}
		db.cond.Wait()
	}
	if db.closed || db.closing {
		return ErrClosed
	}
	if db.bgErr != nil {
		return db.bgErr
	}
	if db.rs.mem.ApproximateSize() > 0 {
		if err := db.sealLocked(); err != nil {
			return err
		}
		db.flushers++
		db.mu.Unlock()
		db.flushImm()
		db.mu.Lock()
		for db.flushers > 0 || db.compacting {
			if db.bgErr != nil {
				return db.bgErr
			}
			db.cond.Wait()
		}
		if db.closed || db.closing {
			return ErrClosed
		}
		if db.bgErr != nil {
			return db.bgErr
		}
	}
	// From here mu is held continuously through link + manifest. A writer
	// that dirtied the fresh memtable during the flush advanced db.seq but
	// not the manifest's LastSeq, so stamping vs.LastSeq() (below) keeps
	// the checkpoint's seq boundary exactly consistent with its tables —
	// concurrent writes are simply not in the checkpoint, and that is the
	// contract. DeleteObsolete cannot unlink a captured table because it
	// needs this mutex.

	if err := os.Mkdir(dst, 0o755); err != nil {
		return err
	}
	// Any failure after the directory exists removes it: a half-written
	// checkpoint would otherwise open as a valid but empty database.
	defer func() {
		if err != nil {
			_ = os.RemoveAll(dst)
		}
	}()

	// The checkpoint manifest must exist BEFORE the links: manifest.Open
	// collects table files it does not know about, so opening it after
	// linking would destroy the links.
	cvs, err := manifest.Open(dst, manifest.Options{})
	if err != nil {
		return err
	}
	edit := &manifest.VersionEdit{}
	for _, metas := range db.vs.Current().Files {
		for _, m := range metas {
			if err := os.Link(manifest.TableFileName(db.dir, m.FileNum), manifest.TableFileName(dst, m.FileNum)); err != nil {
				_ = cvs.Close()
				return fmt.Errorf("basalt: checkpoint link: %w", err)
			}
			edit.AddFiles = append(edit.AddFiles, m)
		}
	}
	if err := syncDir(dst); err != nil {
		_ = cvs.Close()
		return err
	}
	// Stamp the boundary the captured tables actually cover — the
	// manifest's committed LastSeq, not the live counter, which a writer
	// could have advanced past what was flushed.
	edit.SetLastSeq(db.vs.LastSeq())
	if err := cvs.Apply(edit); err != nil {
		_ = cvs.Close()
		return err
	}
	return cvs.Close()
}

// ReplaceWithCheckpoint installs the checkpoint at src as the database at
// dataDir. Neither may be open — enforced by taking dataDir's lock for the
// swap, so a still-running engine there fails the call loudly rather than
// writing into a directory about to be renamed away.
//
// It is safe to blind-retry after a crash: the swap is src → dataDir with
// the old state parked at dataDir+".replaced" until the new state is
// durable. src is verified present first, so a retry after a completed
// install (src already consumed) errors out without touching anything —
// exactly what the raft snapshot-install loop (P3.8) needs.
func ReplaceWithCheckpoint(src, dataDir string) error {
	if _, err := os.Stat(src); err != nil {
		return fmt.Errorf("basalt: checkpoint source: %w", err)
	}
	// Exclude a live engine at dataDir; also blocks a concurrent install.
	if _, err := os.Stat(dataDir); err == nil {
		lock, err := acquireLock(filepath.Join(dataDir, "LOCK"))
		if err != nil {
			return err
		}
		defer func() { _ = lock.Close() }()
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}

	old := dataDir + ".replaced"
	if _, err := os.Stat(dataDir); err == nil {
		if err := os.RemoveAll(old); err != nil {
			return err
		}
		if err := os.Rename(dataDir, old); err != nil {
			return err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.Rename(src, dataDir); err != nil {
		return err
	}
	if err := syncDir(filepath.Dir(filepath.Clean(dataDir))); err != nil {
		return err
	}
	return os.RemoveAll(old)
}
