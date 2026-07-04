package manifest

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/LovRanRan/basalt/internal/wal"
)

const currentName = "CURRENT"

func manifestName(num uint64) string { return fmt.Sprintf("MANIFEST-%06d", num) }

// TableFileName names table file num inside dir; exported for the engine.
func TableFileName(dir string, num uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%06d.sst", num))
}

func parseTableName(name string) (uint64, bool) {
	s, ok := strings.CutSuffix(name, ".sst")
	if !ok || s == "" {
		return 0, false
	}
	n, err := strconv.ParseUint(s, 10, 64)
	return n, err == nil
}

func parseManifestName(name string) (uint64, bool) {
	s, ok := strings.CutPrefix(name, "MANIFEST-")
	if !ok || s == "" {
		return 0, false
	}
	n, err := strconv.ParseUint(s, 10, 64)
	return n, err == nil
}

type Options struct {
	// RotateSize is how far the manifest log must grow past the last
	// snapshot before rotating to a fresh one. Defaults to 1 MiB.
	RotateSize int64

	// failpoint, when non-nil, is invoked between rotation steps; a
	// non-nil return simulates a crash at that step. Tests only.
	failpoint func(step string) error
}

// VersionSet owns the manifest log and the current Version. Callers must
// serialize all mutating methods — the engine's write path is the single
// mutator; Current is lock-free and safe from any goroutine. After any I/O
// error the set is permanently failed (same contract as the WAL writer):
// recovery is reopen-and-replay, never re-submitting an edit — a failed
// Apply does NOT imply the edit missed the log.
type VersionSet struct {
	dir          string
	opts         Options
	cur          atomic.Pointer[Version]
	logNumber    uint64
	nextFileNum  uint64
	lastSeq      uint64
	manifestNum  uint64
	f            *os.File
	size         int64
	snapshotSize int64
	buf          []byte
	err          error
	failpoint    func(step string) error
}

// Open recovers (or freshly creates) the manifest state in dir, always
// rotates to a new snapshot manifest — every open exercises the rotation
// path and implicitly drops a torn tail — and collects obsolete files.
func Open(dir string, opts Options) (*VersionSet, error) {
	if opts.RotateSize <= 0 {
		opts.RotateSize = 1 << 20
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	if err := syncDir(filepath.Dir(filepath.Clean(dir))); err != nil {
		return nil, err
	}
	vs := &VersionSet{dir: dir, opts: opts, nextFileNum: 1, failpoint: opts.failpoint}
	vs.cur.Store(&Version{})

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	cur, err := os.ReadFile(filepath.Join(dir, currentName))
	switch {
	case errors.Is(err, os.ErrNotExist):
		// Only a genuinely fresh directory may start empty. Table files
		// without a CURRENT pointer cannot result from any crash — the
		// engine only writes tables after Open made CURRENT durable — so
		// opening empty would let the collector destroy real data.
		for _, ent := range entries {
			if _, ok := parseTableName(ent.Name()); ok {
				return nil, corruptf("CURRENT missing but table files exist")
			}
		}
	case err != nil:
		return nil, err
	default:
		num, ok := parseManifestName(strings.TrimSpace(string(cur)))
		if !ok {
			return nil, corruptf("CURRENT does not name a manifest: %q", cur)
		}
		if err := vs.replay(num); err != nil {
			return nil, err
		}
	}
	// A crash mid-rotation (or mid-flush, for tables) can orphan files
	// numbered at or above the recovered NextFileNum — that counter was
	// logged before the orphan was allocated. Scan past every file on disk
	// so O_EXCL creates and fresh allocations can never collide.
	for _, ent := range entries {
		var num uint64
		var ok bool
		if num, ok = parseTableName(ent.Name()); !ok {
			num, ok = parseManifestName(ent.Name())
		}
		if ok && num >= vs.nextFileNum {
			vs.nextFileNum = num + 1
		}
	}
	if err := vs.rotate(); err != nil {
		return nil, err
	}
	if _, err := vs.DeleteObsolete(nil); err != nil {
		return nil, err
	}
	return vs, nil
}

// replay rebuilds state from manifest num.
//
// A torn final record is the expected artifact of a crashed append — every
// Apply fsyncs before acknowledging, so a torn record was never acked — and
// is dropped. Honesty about the limits: a record whose frame is complete
// but checksum-failed, sitting exactly at the end of the file, is likewise
// dropped (a crash can persist file size before data); but a damaged record
// with bytes beyond its declared end is provably not a torn tail — it is
// media damage to acknowledged edits, and replay refuses it, because the
// rotation that follows Open would otherwise snapshot the truncated state
// and the collector would permanently delete the tables the lost edits
// reference.
func (vs *VersionSet) replay(num uint64) error {
	buf, err := os.ReadFile(filepath.Join(vs.dir, manifestName(num)))
	if err != nil {
		return err
	}
	v := &Version{}
	clean, err := wal.ScanRecords(buf, func(payload []byte) error {
		e, err := decodeEdit(payload)
		if err != nil {
			return err
		}
		if v, err = applyEdit(v, &e); err != nil {
			return err
		}
		if e.HasLogNumber {
			vs.logNumber = e.LogNumber
		}
		if e.HasNextFileNum {
			vs.nextFileNum = e.NextFileNum
		}
		if e.HasLastSeq {
			vs.lastSeq = e.LastSeq
		}
		return nil
	})
	if err != nil {
		return err
	}
	if tail := buf[clean:]; len(tail) >= wal.RecordHeaderLen {
		declared := uint64(binary.LittleEndian.Uint32(tail[4:8]))
		if uint64(wal.RecordHeaderLen)+declared < uint64(len(tail)) {
			return corruptf("%s: damaged record at offset %d is not a torn tail", manifestName(num), clean)
		}
	}
	vs.cur.Store(v)
	vs.manifestNum = num
	if num >= vs.nextFileNum {
		vs.nextFileNum = num + 1
	}
	return nil
}

func (vs *VersionSet) fail(err error) error {
	if vs.err == nil {
		vs.err = fmt.Errorf("manifest: version set failed, reopen to recover: %w", err)
	}
	return err
}

func (vs *VersionSet) fp(step string) error {
	if vs.failpoint == nil {
		return nil
	}
	if err := vs.failpoint(step); err != nil {
		return vs.fail(err)
	}
	return nil
}

// rotate writes the full current state to a fresh manifest, durably, then
// atomically repoints CURRENT. A crash anywhere in between leaves CURRENT
// naming the old, still-complete manifest.
func (vs *VersionSet) rotate() error {
	num := vs.nextFileNum
	vs.nextFileNum++
	edit := snapshot(vs.Current())
	edit.SetLogNumber(vs.logNumber)
	edit.SetNextFileNum(vs.nextFileNum)
	edit.SetLastSeq(vs.lastSeq)

	path := filepath.Join(vs.dir, manifestName(num))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return vs.fail(err)
	}
	installed := false
	defer func() {
		if !installed {
			_ = f.Close()
		}
	}()
	vs.buf = wal.AppendRecord(vs.buf[:0], edit.encode(nil))
	if _, err := f.Write(vs.buf); err != nil {
		return vs.fail(err)
	}
	if err := vs.fp("write-manifest"); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return vs.fail(err)
	}
	if err := vs.fp("sync-manifest"); err != nil {
		return err
	}
	if err := syncDir(vs.dir); err != nil {
		return vs.fail(err)
	}
	if err := vs.fp("sync-dir"); err != nil {
		return err
	}
	if err := vs.setCurrent(num); err != nil {
		return err
	}
	if vs.f != nil {
		_ = vs.f.Close()
	}
	installed = true
	vs.f = f
	vs.size = int64(len(vs.buf))
	vs.snapshotSize = vs.size
	vs.manifestNum = num
	return nil
}

// setCurrent atomically repoints CURRENT via tmp-write, fsync, rename,
// dir-fsync.
func (vs *VersionSet) setCurrent(num uint64) error {
	tmp := filepath.Join(vs.dir, currentName+".tmp")
	if err := os.WriteFile(tmp, []byte(manifestName(num)+"\n"), 0o644); err != nil {
		return vs.fail(err)
	}
	tf, err := os.OpenFile(tmp, os.O_WRONLY, 0)
	if err != nil {
		return vs.fail(err)
	}
	err = tf.Sync()
	if cerr := tf.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return vs.fail(err)
	}
	if err := vs.fp("sync-tmp"); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(vs.dir, currentName)); err != nil {
		return vs.fail(err)
	}
	if err := vs.fp("rename-current"); err != nil {
		return err
	}
	if err := syncDir(vs.dir); err != nil {
		return vs.fail(err)
	}
	return nil
}

// Apply logs the edit durably, then installs it in memory. Preconditions
// and contract:
//   - files the edit references must already be durable — contents fsynced
//     AND their directory entry fsynced — before Apply is called: the
//     logged record can survive a crash even when Apply never returned;
//   - counters may only advance; a regressing LogNumber or LastSeq is
//     refused as a logic error;
//   - a nil return means the edit is durable. A non-nil return from the
//     write path leaves durability indeterminate and the set failed — the
//     caller's edit object is never mutated, but must not be re-submitted;
//     reopen and rederive instead. Rotation failures after the fsync are
//     not reported against the (already committed) edit: they poison the
//     set and surface on the next call.
func (vs *VersionSet) Apply(e *VersionEdit) error {
	if vs.err != nil {
		return vs.err
	}
	if e.HasLogNumber && e.LogNumber < vs.logNumber {
		return vs.fail(corruptf("log number regression: %d < %d", e.LogNumber, vs.logNumber))
	}
	if e.HasLastSeq && e.LastSeq < vs.lastSeq {
		return vs.fail(corruptf("last seq regression: %d < %d", e.LastSeq, vs.lastSeq))
	}
	newV, err := applyEdit(vs.Current(), e)
	if err != nil {
		return vs.fail(err)
	}
	stamped := *e
	if !stamped.HasLogNumber {
		stamped.SetLogNumber(vs.logNumber)
	}
	if !stamped.HasLastSeq {
		stamped.SetLastSeq(vs.lastSeq)
	}
	stamped.SetNextFileNum(vs.nextFileNum)

	vs.buf = wal.AppendRecord(vs.buf[:0], stamped.encode(nil))
	if _, err := vs.f.Write(vs.buf); err != nil {
		return vs.fail(err)
	}
	if err := vs.f.Sync(); err != nil {
		return vs.fail(err)
	}
	vs.size += int64(len(vs.buf))

	vs.cur.Store(newV)
	vs.logNumber = stamped.LogNumber
	vs.lastSeq = stamped.LastSeq

	// Rotate once the log has grown RotateSize past the last snapshot;
	// anchoring on the snapshot size avoids rewriting a large snapshot on
	// every subsequent edit.
	if vs.size >= vs.opts.RotateSize+vs.snapshotSize {
		_ = vs.rotate()
	}
	return nil
}

// AllocFileNum hands out the next file number; it becomes durable with the
// next Apply (every record carries NextFileNum). Reuse of an orphan's
// number after a crash is impossible because Open scans the directory past
// every on-disk file number; DeleteObsolete merely reclaims the space.
func (vs *VersionSet) AllocFileNum() uint64 {
	n := vs.nextFileNum
	vs.nextFileNum++
	return n
}

// Current returns the live Version; safe from any goroutine, lock-free.
func (vs *VersionSet) Current() *Version { return vs.cur.Load() }

func (vs *VersionSet) LogNumber() uint64   { return vs.logNumber }
func (vs *VersionSet) LastSeq() uint64     { return vs.lastSeq }
func (vs *VersionSet) ManifestNum() uint64 { return vs.manifestNum }

// AdvanceLastSeq raises the in-memory last sequence number; it never
// regresses. Durability comes with the next Apply.
func (vs *VersionSet) AdvanceLastSeq(n uint64) {
	if n > vs.lastSeq {
		vs.lastSeq = n
	}
}

// DeleteObsolete removes table files not in the current version, manifests
// other than the current one, and any leftover CURRENT.tmp, returning the
// removed names. protect names table numbers that are pending — written but
// not yet logged by an edit (a concurrent flush or compaction output) — and
// must include every such file or it will be destroyed here.
func (vs *VersionSet) DeleteObsolete(protect map[uint64]bool) ([]string, error) {
	if vs.err != nil {
		return nil, vs.err
	}
	live := vs.Current().Live()
	entries, err := os.ReadDir(vs.dir)
	if err != nil {
		return nil, err
	}
	var removed []string
	for _, ent := range entries {
		name := ent.Name()
		var doomed bool
		if num, ok := parseTableName(name); ok {
			doomed = !live[num] && !protect[num]
		} else if num, ok := parseManifestName(name); ok {
			doomed = num != vs.manifestNum
		} else if name == currentName+".tmp" {
			doomed = true
		}
		if doomed {
			if err := os.Remove(filepath.Join(vs.dir, name)); err != nil {
				return removed, err
			}
			removed = append(removed, name)
		}
	}
	return removed, syncDir(vs.dir)
}

func (vs *VersionSet) Close() error {
	if vs.f == nil {
		return nil
	}
	err := vs.f.Sync()
	if cerr := vs.f.Close(); err == nil {
		err = cerr
	}
	vs.f = nil
	if err != nil {
		return vs.fail(err)
	}
	return nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = d.Sync()
	if cerr := d.Close(); err == nil {
		err = cerr
	}
	return err
}
