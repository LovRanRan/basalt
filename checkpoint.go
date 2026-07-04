package basalt

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/LovRanRan/basalt/internal/manifest"
)

// Checkpoint writes a consistent point-in-time copy of the database into
// dst, which must not exist yet. The memtable is flushed first, so the
// checkpoint is entirely tables plus a fresh manifest — no WAL — and every
// table is hard-linked, not copied: checkpoints are cheap and immutable
// (table files are never modified in place, and compaction unlinking the
// original names cannot disturb the links).
//
// The checkpoint is itself a complete database: Open it directly for a
// stable snapshot iterator, or install it with ReplaceWithCheckpoint.
// Checkpoint blocks writes for its duration (link + manifest write, no data
// copies); background work is drained first.
func (db *DB) Checkpoint(dst string) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed || db.closing {
		return errClosed
	}
	// Drain background work; holding mu afterwards excludes new flushes,
	// compactions, and — critically — DeleteObsolete unlinking a table
	// between the version capture and its hard link.
	for db.flushers > 0 || db.compacting || db.rs.imm != nil {
		if db.bgErr != nil {
			return db.bgErr
		}
		db.cond.Wait()
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
		if db.bgErr != nil {
			return db.bgErr
		}
	}

	// Re-check after every wait: Close may have begun while mu was
	// released inside cond.Wait or the inline flush.
	if db.closed || db.closing {
		return errClosed
	}
	if err := os.Mkdir(dst, 0o755); err != nil {
		return err
	}
	// The checkpoint manifest must exist BEFORE the links: manifest.Open
	// collects table files it does not know about, so opening it after
	// linking would destroy the links.
	cvs, err := manifest.Open(dst, manifest.Options{})
	if err != nil {
		return err
	}
	edit := &manifest.VersionEdit{}
	v := db.vs.Current()
	for _, metas := range v.Files {
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
	edit.SetLastSeq(db.seq.Load())
	if err := cvs.Apply(edit); err != nil {
		_ = cvs.Close()
		return err
	}
	return cvs.Close()
}

// ReplaceWithCheckpoint installs the checkpoint at src as the database at
// dataDir. Neither may be open. The old state is renamed aside, the
// checkpoint moves in via rename, and the old directory is removed only
// after the swap is durable. A crash between the two renames leaves
// dataDir missing and both src and dataDir+".replaced" intact — the caller
// (raft snapshot installation, P3.8) retries the install.
func ReplaceWithCheckpoint(src, dataDir string) error {
	old := dataDir + ".replaced"
	if err := os.RemoveAll(old); err != nil {
		return err
	}
	if _, err := os.Stat(dataDir); err == nil {
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
