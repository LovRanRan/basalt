package basalt

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/LovRanRan/basalt/internal/base"
	"github.com/LovRanRan/basalt/internal/manifest"
	"github.com/LovRanRan/basalt/internal/memtable"
	"github.com/LovRanRan/basalt/internal/sstable"
	"github.com/LovRanRan/basalt/internal/wal"
)

// ErrNotFound reports that a key has no live value (absent or deleted).
var ErrNotFound = errors.New("basalt: key not found")

// ErrBatchTooLarge rejects a write whose encoded batch exceeds the WAL's
// record limit. The engine stays fully usable: nothing was written.
var ErrBatchTooLarge = errors.New("basalt: batch exceeds max WAL record size")

var errClosed = errors.New("basalt: db is closed")

type Options struct {
	// MemTableSize is the write-buffer size that triggers a flush to L0.
	// Defaults to 4 MiB.
	MemTableSize int64
	// L0CompactThreshold is the L0 file count that triggers compaction
	// into L1. Default 4.
	L0CompactThreshold int
	// L0StopThreshold is the L0 file count at which writes stall until
	// compaction catches up. Default 12.
	L0StopThreshold int
	// LevelBytesBase is L1's byte budget; each deeper level gets 10x.
	// Default 10 MiB.
	LevelBytesBase int64
	// TargetFileSize splits compaction outputs. Default 2 MiB.
	TargetFileSize int64
	// DisableWALSync skips the per-record WAL fsync (group durability
	// only): a crash may lose recent acknowledged writes — never ordering
	// or integrity. For tests and benchmarks.
	DisableWALSync bool
}

func (o *Options) defaults() {
	if o.MemTableSize <= 0 {
		o.MemTableSize = 4 << 20
	}
	if o.L0CompactThreshold <= 0 {
		o.L0CompactThreshold = 4
	}
	if o.L0StopThreshold <= 0 {
		o.L0StopThreshold = 12
	}
	if o.LevelBytesBase <= 0 {
		o.LevelBytesBase = 10 << 20
	}
	if o.TargetFileSize <= 0 {
		o.TargetFileSize = 2 << 20
	}
}

// tableHandle is one open table file, shared by every readState (and
// compaction) that includes it and refcounted so the file closes exactly
// when the last user releases it — files unlinked by compaction stay
// readable for pinned iterators until then.
type tableHandle struct {
	num               uint64
	smallest, largest []byte // user-key bounds
	f                 *os.File
	r                 *sstable.Reader
	refs              atomic.Int32
}

func (h *tableHandle) ref() { h.refs.Add(1) }

func (h *tableHandle) unref() {
	n := h.refs.Add(-1)
	if n < 0 {
		panic("basalt: table handle released below zero")
	}
	if n == 0 {
		_ = h.f.Close()
	}
}

// DB is the storage engine: writes go WAL-first into a memtable under one
// monotonic sequence counter; full memtables seal and flush to L0 tables in
// the background; reads check the mutable memtable, the flushing one, then
// L0 newest-first. Every acknowledged write is durable (the WAL syncs per
// record) and survives any crash.
//
// Put, Delete, and Get are safe for concurrent use. Close must not race
// other calls.
type DB struct {
	dir  string
	opts Options

	vs  *manifest.VersionSet
	wal *wal.Writer

	// seq is the newest published sequence number: it is stored only
	// after the write is in the memtable, so a reader that snapshots it
	// sees every write it covers (the happens-before edge the memtable
	// contract requires).
	seq atomic.Uint64

	mu         sync.Mutex
	cond       *sync.Cond
	rs         *readState                 // current read view; swapped wholesale, never mutated
	tables     map[uint64]*tableHandle    // every open handle; each entry holds one cache ref
	pending    map[uint64]bool            // allocated table numbers written but not yet in an edit; shields them from the collector
	compactPtr [manifest.NumLevels][]byte // per-level rotation cursor (in-memory; resets at open)
	immSeq     uint64                     // newest seq sealed into imm
	immBound   uint64                     // first wal segment owned by mem (all before hold imm-or-flushed data)
	encBuf     []byte
	bgErr      error
	flushers   int  // live flushImm goroutines that may still touch vs/wal/rs
	compacting bool // one background compaction at a time
	closing    bool // Close has begun: no new background work
	closed     bool
	lock       *os.File // held flock; two engines on one dir destroy data

	// Crash-injection hooks (tests only): returning an error simulates
	// dying at that point in the flush/compaction protocol.
	hookAfterTableWrite    func() error
	hookAfterApply         func() error
	hookBeforeCompactApply func() error
	hookAfterCompactApply  func() error
}

func Open(dir string, opts Options) (*DB, error) {
	opts.defaults()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	// One engine per directory, enforced across processes: a second Open
	// would rotate the manifest and truncate the live WAL out from under
	// the first — silent destruction of acknowledged data, no crash
	// required.
	lock, err := acquireLock(filepath.Join(dir, "LOCK"))
	if err != nil {
		return nil, err
	}
	vs, err := manifest.Open(dir, manifest.Options{})
	if err != nil {
		_ = lock.Close()
		return nil, err
	}
	db := &DB{dir: dir, opts: opts, vs: vs, lock: lock, tables: map[uint64]*tableHandle{}, pending: map[uint64]bool{}}
	db.cond = sync.NewCond(&db.mu)

	ok := false
	defer func() {
		if !ok {
			db.releaseFiles()
		}
	}()

	if err := db.installVersionLocked(memtable.New(), nil); err != nil {
		return nil, err
	}

	walDir := filepath.Join(dir, "wal")
	// Segments below the manifest's log number hold only flushed data;
	// re-applying them would duplicate internal keys.
	if err := wal.DeleteSegmentsBelow(walDir, vs.LogNumber()); err != nil {
		return nil, err
	}
	maxSeq := vs.LastSeq()
	err = wal.Replay(walDir, func(payload []byte) error {
		b, err := wal.DecodeBatch(payload)
		if err != nil {
			return err
		}
		for i, op := range b.Ops {
			s := b.BaseSeq + uint64(i)
			db.rs.mem.Add(op.UserKey, s, op.Kind, op.Value)
			if s > maxSeq {
				maxSeq = s
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	db.seq.Store(maxSeq)
	vs.AdvanceLastSeq(maxSeq)

	syncPolicy := wal.SyncEveryRecord
	if opts.DisableWALSync {
		syncPolicy = wal.SyncManual
	}
	if db.wal, err = wal.OpenWriter(wal.Options{Dir: walDir, Sync: syncPolicy}); err != nil {
		return nil, err
	}
	db.mu.Lock()
	db.maybeScheduleCompactionLocked()
	db.mu.Unlock()
	ok = true
	return db, nil
}

// installVersionLocked mirrors the manifest's current version into a fresh
// readState: handles for new files open lazily into the cache; files that
// left the version lose their cache reference and close once the last
// pinned reader releases. Callers hold mu (or, at Open, own the DB
// exclusively).
func (db *DB) installVersionLocked(mem, imm *memtable.MemTable) error {
	v := db.vs.Current()
	var levels [manifest.NumLevels][]*tableHandle
	seen := make(map[uint64]bool, len(db.tables))
	for lvl, metas := range v.Files {
		for _, m := range metas {
			h, okh := db.tables[m.FileNum]
			if !okh {
				var err error
				if h, err = db.openTable(m); err != nil {
					return err
				}
				db.tables[m.FileNum] = h
			}
			levels[lvl] = append(levels[lvl], h)
			seen[m.FileNum] = true
		}
	}
	for num, h := range db.tables {
		if !seen[num] {
			delete(db.tables, num)
			h.unref()
		}
	}
	old := db.rs
	db.rs = newReadState(mem, imm, levels)
	if old != nil {
		old.release()
	}
	return nil
}

func acquireLock(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("basalt: database is locked by another process: %w", err)
	}
	return f, nil
}

func (db *DB) releaseFiles() {
	if db.rs != nil {
		db.rs.release()
	}
	for num, h := range db.tables {
		delete(db.tables, num)
		h.unref()
	}
	if db.wal != nil {
		_ = db.wal.Close()
	}
	_ = db.vs.Close()
	_ = db.lock.Close()
}

// openTable opens table meta.FileNum with one reference owned by the
// caller.
func (db *DB) openTable(meta manifest.FileMeta) (*tableHandle, error) {
	f, err := os.Open(manifest.TableFileName(db.dir, meta.FileNum))
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	r, err := sstable.NewReader(f, st.Size())
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("table %06d: %w", meta.FileNum, err)
	}
	h := &tableHandle{
		num:      meta.FileNum,
		smallest: append([]byte(nil), base.UserKey(meta.Smallest)...),
		largest:  append([]byte(nil), base.UserKey(meta.Largest)...),
		f:        f,
		r:        r,
	}
	h.refs.Store(1)
	return h, nil
}

func (db *DB) Put(key, value []byte) error {
	var b wal.Batch
	b.Put(key, value)
	return db.write(&b)
}

func (db *DB) Delete(key []byte) error {
	var b wal.Batch
	b.Delete(key)
	return db.write(&b)
}

// write commits one batch: WAL append (synced), memtable apply under
// consecutive seqnos, then seq publication. A write error poisons the
// engine — the WAL contract after a failed append is reopen-to-recover.
func (db *DB) write(b *wal.Batch) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	switch {
	case db.closed:
		return errClosed
	case db.bgErr != nil:
		return db.bgErr
	}
	if err := db.makeRoomLocked(); err != nil {
		return err
	}
	prev := db.seq.Load()
	b.BaseSeq = prev + 1
	db.encBuf = b.Encode(db.encBuf[:0])
	if _, err := db.wal.Append(db.encBuf); err != nil {
		if errors.Is(err, wal.ErrRecordTooLarge) {
			// Nothing was written; the engine stays healthy.
			return fmt.Errorf("%w: %v", ErrBatchTooLarge, err)
		}
		db.bgErr = fmt.Errorf("basalt: wal append: %w", err)
		return db.bgErr
	}
	for i, op := range b.Ops {
		db.rs.mem.Add(op.UserKey, b.BaseSeq+uint64(i), op.Kind, op.Value)
	}
	db.seq.Store(prev + uint64(len(b.Ops)))
	return nil
}

// makeRoomLocked seals the memtable once it is full, waiting for any
// in-flight flush first, and hands the sealed table to a background flush.
func (db *DB) makeRoomLocked() error {
	if db.rs.mem.ApproximateSize() < db.opts.MemTableSize {
		return nil
	}
	for db.rs.imm != nil && db.bgErr == nil {
		db.cond.Wait()
	}
	if db.bgErr != nil {
		return db.bgErr
	}
	// Stall while L0 is over the stop threshold: unbounded L0 growth makes
	// reads O(files) and compaction is the only cure.
	for len(db.rs.levels[0]) >= db.opts.L0StopThreshold && db.bgErr == nil && !db.closing {
		db.maybeScheduleCompactionLocked()
		db.cond.Wait()
	}
	if db.bgErr != nil {
		return db.bgErr
	}
	if db.rs.mem.ApproximateSize() < db.opts.MemTableSize {
		return nil // someone else made room while we waited
	}
	if err := db.sealLocked(); err != nil {
		return err
	}
	db.flushers++
	go db.flushImm()
	return nil
}

// sealLocked freezes mem into imm — installing a fresh read state so live
// readers keep their pinned view — and rotates the WAL so that every
// segment before the rotation holds only sealed-or-flushed data.
func (db *DB) sealLocked() error {
	if err := db.wal.Rotate(); err != nil {
		db.bgErr = fmt.Errorf("basalt: wal rotate: %w", err)
		return db.bgErr
	}
	old := db.rs
	db.rs = newReadState(memtable.New(), old.mem, old.levels)
	old.release()
	db.immSeq = db.seq.Load()
	db.immBound = db.wal.SegmentID()
	return nil
}

// flushImm writes imm as an L0 table, commits it to the manifest with the
// new log-number boundary, installs the reader, and prunes the WAL. On any
// failure the engine poisons itself; the WAL retains everything, so reopen
// recovers every acknowledged write.
func (db *DB) flushImm() {
	db.mu.Lock()
	imm, sealSeq, bound := db.rs.imm, db.immSeq, db.immBound
	num := db.vs.AllocFileNum()
	db.pending[num] = true
	db.mu.Unlock()

	fail := func(err error) {
		db.mu.Lock()
		if db.bgErr == nil {
			db.bgErr = fmt.Errorf("basalt: flush: %w", err)
		}
		delete(db.pending, num)
		db.flushers--
		db.cond.Broadcast()
		db.mu.Unlock()
	}

	meta, err := db.writeTable(num, imm)
	if err != nil {
		fail(err)
		return
	}
	if db.hookAfterTableWrite != nil {
		if err := db.hookAfterTableWrite(); err != nil {
			fail(err)
			return
		}
	}
	h, err := db.openTable(meta)
	if err != nil {
		fail(err)
		return
	}

	db.mu.Lock()
	edit := &manifest.VersionEdit{AddFiles: []manifest.FileMeta{meta}}
	edit.SetLogNumber(bound)
	edit.SetLastSeq(sealSeq)
	if err := db.vs.Apply(edit); err != nil {
		db.mu.Unlock()
		h.unref()
		fail(err)
		return
	}
	db.tables[h.num] = h
	delete(db.pending, num)
	if err := db.installVersionLocked(db.rs.mem, nil); err != nil {
		db.mu.Unlock()
		fail(err)
		return
	}
	db.maybeScheduleCompactionLocked()

	if db.hookAfterApply != nil {
		if err := db.hookAfterApply(); err != nil {
			if db.bgErr == nil {
				db.bgErr = fmt.Errorf("basalt: flush: %w", err)
			}
			db.flushers--
			db.cond.Broadcast()
			db.mu.Unlock()
			return
		}
	}
	// Writers can proceed as soon as imm clears; the prune happens outside
	// mu — per-segment dir fsyncs must not stall the read/write hot path.
	// The package-level form touches only dead segments, so it needs no
	// serialization with the writer.
	db.cond.Broadcast()
	db.mu.Unlock()

	pruneErr := wal.DeleteSegmentsBelow(filepath.Join(db.dir, "wal"), bound)

	db.mu.Lock()
	if pruneErr != nil && db.bgErr == nil {
		db.bgErr = fmt.Errorf("basalt: wal prune: %w", pruneErr)
	}
	db.flushers--
	db.cond.Broadcast()
	db.mu.Unlock()
}

// writeTable streams a memtable into table file num. The file is durable —
// contents and directory entry — before this returns, which the manifest's
// Apply contract requires.
func (db *DB) writeTable(num uint64, mt *memtable.MemTable) (manifest.FileMeta, error) {
	path := manifest.TableFileName(db.dir, num)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return manifest.FileMeta{}, err
	}
	bw := bufio.NewWriterSize(f, 64<<10)
	w := sstable.NewWriter(bw, sstable.WriterOptions{})
	it := mt.NewIterator()
	for it.First(); it.Valid(); it.Next() {
		if err := w.Add(it.Key(), it.Value()); err != nil {
			_ = f.Close()
			return manifest.FileMeta{}, err
		}
	}
	props, err := w.Finish()
	if err != nil {
		_ = f.Close()
		return manifest.FileMeta{}, err
	}
	if err := bw.Flush(); err != nil {
		_ = f.Close()
		return manifest.FileMeta{}, err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return manifest.FileMeta{}, err
	}
	if err := f.Close(); err != nil {
		return manifest.FileMeta{}, err
	}
	if err := syncDir(db.dir); err != nil {
		return manifest.FileMeta{}, err
	}
	return manifest.FileMeta{
		Level:    0,
		FileNum:  num,
		Size:     props.FileSize,
		Smallest: props.Smallest,
		Largest:  props.Largest,
	}, nil
}

// Get returns the newest value for key. Deleted or absent keys return
// ErrNotFound. The returned slice is the caller's to keep.
//
// The point-get path and Scan share one semantics: both resolve the newest
// version visible at the acquired sequence snapshot, layer by layer; Get
// merely short-circuits at the first layer that knows the key.
func (db *DB) Get(key []byte) ([]byte, error) {
	rs, seq, err := db.acquire()
	if err != nil {
		return nil, err
	}
	defer rs.release()

	if v, kind, ok := rs.mem.Get(key, seq); ok {
		return finish(v, kind, true)
	}
	if rs.imm != nil {
		if v, kind, ok := rs.imm.Get(key, seq); ok {
			return finish(v, kind, true)
		}
	}
	for _, h := range rs.levels[0] { // newest first; ranges overlap
		v, kind, ok, err := h.r.Get(key, seq)
		if err != nil {
			return nil, err
		}
		if ok {
			return finish(v, kind, false)
		}
	}
	for lvl := 1; lvl < manifest.NumLevels; lvl++ {
		files := rs.levels[lvl]
		if len(files) == 0 {
			continue
		}
		// Disjoint and sorted: at most one candidate file per level.
		i := sort.Search(len(files), func(i int) bool { return bytes.Compare(files[i].largest, key) >= 0 })
		if i == len(files) || bytes.Compare(files[i].smallest, key) > 0 {
			continue
		}
		v, kind, ok, err := files[i].r.Get(key, seq)
		if err != nil {
			return nil, err
		}
		if ok {
			return finish(v, kind, false)
		}
	}
	return nil, ErrNotFound
}

// finish interprets a found entry; memtable values alias engine memory and
// are copied, table values are already caller-owned.
func finish(v []byte, kind base.Kind, copyValue bool) ([]byte, error) {
	if kind == base.KindDelete {
		return nil, ErrNotFound
	}
	if copyValue {
		v = append([]byte(nil), v...)
	}
	return v, nil
}

// Close flushes everything: it drains any in-flight flush, flushes the
// remaining memtable, and closes the WAL, manifest, and table files. After
// a clean Close the WAL holds no data and reopen needs no replay.
func (db *DB) Close() error {
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return nil
	}
	db.closing = true // no new background work schedules from here on
	// Drain the background goroutines themselves, not just their slots: a
	// write-path failure can poison the engine while a flush or compaction
	// is still mid-I/O, and tearing down vs/wal/tables under it would be a
	// data race.
	for db.flushers > 0 || db.compacting || (db.rs.imm != nil && db.bgErr == nil) {
		db.cond.Wait()
	}
	if db.bgErr == nil && db.rs.mem.ApproximateSize() > 0 {
		if err := db.sealLocked(); err == nil {
			db.flushers++
			db.mu.Unlock()
			db.flushImm()
			db.mu.Lock()
			for db.flushers > 0 || db.compacting {
				db.cond.Wait()
			}
		}
	}
	db.closed = true
	firstErr := db.bgErr
	// Drop the DB's own references: the readState pin and the table
	// cache. Files close when the last pinned reader releases, so an
	// iterator that outlives Close keeps working until its own Close.
	db.rs.release()
	for num, h := range db.tables {
		delete(db.tables, num)
		h.unref()
	}
	db.mu.Unlock()

	if err := db.wal.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := db.vs.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := db.lock.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
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
