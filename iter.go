package basalt

import (
	"bytes"
	"fmt"
	"sync/atomic"

	"github.com/LovRanRan/basalt/internal/base"
	"github.com/LovRanRan/basalt/internal/manifest"
	"github.com/LovRanRan/basalt/internal/memtable"
)

// readState is the engine's read view: the memtables and table set that a
// reader or iterator pins for its lifetime. Pinning is refcounted so a
// mid-scan flush or compaction installs a new state without invalidating
// live readers; the state refs every table handle it includes and unrefs
// them when the last pin releases, closing files retired by compaction
// only once nobody can still read them.
type readState struct {
	mem    *memtable.MemTable
	imm    *memtable.MemTable                 // may be nil
	levels [manifest.NumLevels][]*tableHandle // L0 newest-first; deeper levels key-sorted, disjoint
	refs   atomic.Int32
}

func newReadState(mem, imm *memtable.MemTable, levels [manifest.NumLevels][]*tableHandle) *readState {
	rs := &readState{mem: mem, imm: imm, levels: levels}
	for _, level := range rs.levels {
		for _, h := range level {
			h.ref()
		}
	}
	rs.refs.Store(1) // the DB's own reference
	return rs
}

func (rs *readState) release() {
	n := rs.refs.Add(-1)
	if n < 0 {
		panic("basalt: readState released below zero")
	}
	if n == 0 {
		for _, level := range rs.levels {
			for _, h := range level {
				h.unref()
			}
		}
	}
}

// acquire pins the current read state together with the sequence snapshot.
// Capturing both under mu gives the invariant readers rely on: every write
// with seqno <= seq is present in the pinned structures.
func (db *DB) acquire() (*readState, uint64, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return nil, 0, ErrClosed
	}
	rs := db.rs
	rs.refs.Add(1)
	return rs, db.seq.Load(), nil
}

// Iterator yields live user keys in ascending order within [start, end).
// It reads a snapshot: writes acknowledged after Scan returned are
// invisible, and concurrent flushes do not disturb it. Key and Value alias
// engine memory: valid only until the next call to Next, and never to be
// modified — copy to retain. Callers must Close the iterator to release its
// pinned read state, must Close all iterators before closing the DB (a
// live iterator's table reads fail once the DB closes), and must check
// Error whenever the iterator goes invalid. Not safe for concurrent use.
type Iterator struct {
	rs     *readState
	m      *mergingIter
	seq    uint64
	end    []byte
	ukBuf  []byte
	err    error
	valid  bool
	closed bool
}

// Scan returns an iterator over [start, end); a nil start begins at the
// first key, a nil end runs to the last.
func (db *DB) Scan(start, end []byte) *Iterator {
	rs, seq, err := db.acquire()
	if err != nil {
		return &Iterator{err: err, closed: true}
	}
	children := make([]internalIterator, 0, 8)
	children = append(children, rs.mem.NewIterator())
	if rs.imm != nil {
		children = append(children, rs.imm.NewIterator())
	}
	for _, level := range rs.levels {
		for _, h := range level {
			children = append(children, h.r.NewIterator())
		}
	}
	it := &Iterator{rs: rs, m: newMergingIter(children), seq: seq}
	if end != nil {
		// Preserve non-nil-ness: a non-nil EMPTY end is the empty range
		// (every key is >= ""), not an unbounded scan.
		it.end = append(make([]byte, 0, len(end)), end...)
	}
	if start == nil {
		it.m.First()
	} else {
		it.m.SeekGE(base.AppendSeekKey(nil, start, seq))
	}
	it.findNext()
	return it
}

// findNext advances the merge to the newest visible, non-deleted version of
// the next user key inside the range.
func (it *Iterator) findNext() {
	it.valid = false
	for it.err == nil && it.m.Valid() {
		ik, err := base.DecodeInternalKey(it.m.Key())
		if err != nil {
			it.err = fmt.Errorf("basalt: %w", err)
			return
		}
		if it.end != nil && bytes.Compare(ik.UserKey, it.end) >= 0 {
			return // range exhausted
		}
		if ik.Seq > it.seq {
			it.m.Next() // version newer than the snapshot
			continue
		}
		// Versions of one user key are adjacent, newest first: the first
		// visible one is authoritative.
		if ik.Kind == base.KindDelete {
			it.skipUserKey(ik.UserKey)
			continue
		}
		it.valid = true
		return
	}
	if it.err == nil {
		it.err = it.m.Error()
	}
}

// skipUserKey advances past every remaining version of uk.
func (it *Iterator) skipUserKey(uk []byte) {
	it.ukBuf = append(it.ukBuf[:0], uk...)
	for it.m.Valid() && bytes.Equal(base.UserKey(it.m.Key()), it.ukBuf) {
		it.m.Next()
	}
}

func (it *Iterator) Next() {
	if !it.Valid() {
		panic("basalt: Next on invalid iterator")
	}
	it.skipUserKey(base.UserKey(it.m.Key()))
	it.findNext()
}

func (it *Iterator) Valid() bool { return it.valid && it.err == nil }

// Key returns the current user key; Value its value. Both are valid only
// until the next call to Next.
func (it *Iterator) Key() []byte   { return base.UserKey(it.m.Key()) }
func (it *Iterator) Value() []byte { return it.m.Value() }
func (it *Iterator) Error() error  { return it.err }

// Close releases the pinned read state; it is idempotent and must be
// called exactly when done, or the state (and, after P1.9, its files)
// leaks.
func (it *Iterator) Close() {
	if !it.closed {
		it.closed = true
		it.valid = false
		it.rs.release()
	}
}
