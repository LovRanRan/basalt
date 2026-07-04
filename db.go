package basalt

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
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

type Options struct {
	// MemTableSize is the write-buffer size that triggers a flush to L0.
	// Defaults to 4 MiB.
	MemTableSize int64
}

func (o *Options) defaults() {
	if o.MemTableSize <= 0 {
		o.MemTableSize = 4 << 20
	}
}

type tableHandle struct {
	num uint64
	f   *os.File
	r   *sstable.Reader
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

	mu       sync.Mutex
	cond     *sync.Cond
	mem      *memtable.MemTable
	imm      *memtable.MemTable
	immSeq   uint64         // newest seq sealed into imm
	immBound uint64         // first wal segment owned by mem (all before hold imm-or-flushed data)
	l0       []*tableHandle // newest first; replaced wholesale, never mutated
	encBuf   []byte
	bgErr    error
	flushing bool // a flushImm goroutine may still touch vs/wal/l0
	closed   bool
	lock     *os.File // held flock; two engines on one dir destroy data

	// Crash-injection hooks (tests only): returning an error simulates
	// dying at that point in the flush protocol.
	hookAfterTableWrite func() error
	hookAfterApply      func() error
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
	db := &DB{dir: dir, opts: opts, vs: vs, mem: memtable.New(), lock: lock}
	db.cond = sync.NewCond(&db.mu)

	ok := false
	defer func() {
		if !ok {
			db.releaseFiles()
		}
	}()

	for _, meta := range vs.Current().Files[0] { // already newest-first
		h, err := db.openTable(meta.FileNum)
		if err != nil {
			return nil, err
		}
		db.l0 = append(db.l0, h)
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
			db.mem.Add(op.UserKey, s, op.Kind, op.Value)
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

	if db.wal, err = wal.OpenWriter(wal.Options{Dir: walDir, Sync: wal.SyncEveryRecord}); err != nil {
		return nil, err
	}
	ok = true
	return db, nil
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
	for _, h := range db.l0 {
		_ = h.f.Close()
	}
	if db.wal != nil {
		_ = db.wal.Close()
	}
	_ = db.vs.Close()
	_ = db.lock.Close()
}

func (db *DB) openTable(num uint64) (*tableHandle, error) {
	f, err := os.Open(manifest.TableFileName(db.dir, num))
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
		return nil, fmt.Errorf("table %06d: %w", num, err)
	}
	return &tableHandle{num: num, f: f, r: r}, nil
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
		return errors.New("basalt: db is closed")
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
		db.mem.Add(op.UserKey, b.BaseSeq+uint64(i), op.Kind, op.Value)
	}
	db.seq.Store(prev + uint64(len(b.Ops)))
	return nil
}

// makeRoomLocked seals the memtable once it is full, waiting for any
// in-flight flush first, and hands the sealed table to a background flush.
func (db *DB) makeRoomLocked() error {
	if db.mem.ApproximateSize() < db.opts.MemTableSize {
		return nil
	}
	for db.imm != nil && db.bgErr == nil {
		db.cond.Wait()
	}
	if db.bgErr != nil {
		return db.bgErr
	}
	if db.mem.ApproximateSize() < db.opts.MemTableSize {
		return nil // someone else made room while we waited
	}
	if err := db.sealLocked(); err != nil {
		return err
	}
	db.flushing = true
	go db.flushImm()
	return nil
}

// sealLocked freezes mem into imm and rotates the WAL so that every
// segment before the rotation holds only sealed-or-flushed data.
func (db *DB) sealLocked() error {
	if err := db.wal.Rotate(); err != nil {
		db.bgErr = fmt.Errorf("basalt: wal rotate: %w", err)
		return db.bgErr
	}
	db.imm = db.mem
	db.mem = memtable.New()
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
	imm, sealSeq, bound := db.imm, db.immSeq, db.immBound
	num := db.vs.AllocFileNum()
	db.mu.Unlock()

	fail := func(err error) {
		db.mu.Lock()
		if db.bgErr == nil {
			db.bgErr = fmt.Errorf("basalt: flush: %w", err)
		}
		db.flushing = false
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
	h, err := db.openTable(num)
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
		_ = h.f.Close()
		fail(err)
		return
	}
	l0 := make([]*tableHandle, 0, len(db.l0)+1)
	l0 = append(l0, h)
	l0 = append(l0, db.l0...)
	db.l0 = l0
	db.imm = nil

	if db.hookAfterApply != nil {
		if err := db.hookAfterApply(); err != nil {
			if db.bgErr == nil {
				db.bgErr = fmt.Errorf("basalt: flush: %w", err)
			}
			db.flushing = false
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
	db.flushing = false
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
func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return nil, errors.New("basalt: db is closed")
	}
	// Capture seq together with the structure snapshot, under mu: every
	// write with seqno <= seq is present in the captured structures. This
	// is exactly the invariant P1.8's read snapshots will build on.
	mem, imm, l0 := db.mem, db.imm, db.l0
	seq := db.seq.Load()
	db.mu.Unlock()

	if v, kind, ok := mem.Get(key, seq); ok {
		return finish(v, kind, true)
	}
	if imm != nil {
		if v, kind, ok := imm.Get(key, seq); ok {
			return finish(v, kind, true)
		}
	}
	for _, h := range l0 {
		v, kind, ok, err := h.r.Get(key, seq)
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
	// Drain the flush goroutine itself, not just the imm slot: a write-path
	// failure can poison the engine while a flush is still mid-I/O, and
	// tearing down vs/wal/l0 under it would be a data race.
	for db.flushing || (db.imm != nil && db.bgErr == nil) {
		db.cond.Wait()
	}
	if db.bgErr == nil && db.mem.ApproximateSize() > 0 {
		if err := db.sealLocked(); err == nil {
			db.flushing = true
			db.mu.Unlock()
			db.flushImm()
			db.mu.Lock()
			for db.flushing {
				db.cond.Wait()
			}
		}
	}
	db.closed = true
	firstErr := db.bgErr
	db.mu.Unlock()

	if err := db.wal.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := db.vs.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	for _, h := range db.l0 {
		if err := h.f.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
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
