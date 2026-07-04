// Package memtable provides the engine's in-memory write buffer: an
// insert-only skiplist over encoded internal keys, safe for exactly one
// writer and any number of concurrent readers.
package memtable

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"sync/atomic"

	"github.com/LovRanRan/basalt/internal/base"
)

const (
	maxHeight    = 12
	branchFactor = 4

	// Approximate per-node bookkeeping charged to ApproximateSize, on top
	// of key and value bytes: node struct plus slice headers, and one
	// atomic pointer per level.
	nodeOverhead = 64
	ptrSize      = 8
)

// node links are published with atomic stores and traversed with atomic
// loads; a node's own fields are immutable once the node is reachable.
type node struct {
	key   []byte
	value []byte
	next  []atomic.Pointer[node]
}

// MemTable is an insert-only skiplist keyed by encoded internal keys in
// base.Compare order, so all versions of a user key sit adjacent, newest
// first. Entries are never updated in place — a newer write of the same user
// key simply sorts earlier — and tombstones are ordinary KindDelete entries.
//
// Concurrency contract: exactly one goroutine may call Add at a time; Get
// and iterators may run concurrently with that writer. Readers observe every
// entry whose Add happens-before their operation — i.e. when Add's return is
// ordered before the read by some synchronization (mutex, channel, or
// atomic, such as the engine publishing its last acked sequence number);
// timing alone establishes no ordering.
type MemTable struct {
	head   *node
	height atomic.Int32
	size   atomic.Int64
}

func New() *MemTable {
	m := &MemTable{head: &node{next: make([]atomic.Pointer[node], maxHeight)}}
	m.height.Store(1)
	return m
}

func randomHeight() int {
	h := 1
	for h < maxHeight && rand.IntN(branchFactor) == 0 {
		h++
	}
	return h
}

// findGE returns the first node whose key is >= key, or nil. When preds is
// non-nil it also records, per level, the rightmost node strictly before
// key; levels above the current height are left untouched.
func (m *MemTable) findGE(key []byte, preds *[maxHeight]*node) *node {
	x := m.head
	level := int(m.height.Load()) - 1
	for {
		next := x.next[level].Load()
		if next != nil && base.Compare(next.key, key) < 0 {
			x = next
			continue
		}
		if preds != nil {
			preds[level] = x
		}
		if level == 0 {
			return next
		}
		level--
	}
}

// Add inserts (userKey, seq, kind, value). value is copied. Internal keys
// must be unique — the engine assigns every write a fresh sequence number —
// so inserting an exact duplicate panics.
func (m *MemTable) Add(userKey []byte, seq uint64, kind base.Kind, value []byte) {
	key := base.AppendInternalKey(nil, userKey, seq, kind)
	var preds [maxHeight]*node
	if next := m.findGE(key, &preds); next != nil && base.Compare(next.key, key) == 0 {
		panic(fmt.Sprintf("memtable: duplicate internal key %x", key))
	}

	h := randomHeight()
	if cur := int(m.height.Load()); h > cur {
		for i := cur; i < h; i++ {
			preds[i] = m.head
		}
		// Readers that load the new height before the new levels are
		// spliced see nil head pointers there and simply descend.
		m.height.Store(int32(h))
	}

	n := &node{key: key, value: slices.Clone(value), next: make([]atomic.Pointer[node], h)}
	for i := 0; i < h; i++ {
		n.next[i].Store(preds[i].next[i].Load())
	}
	// Publish bottom-up: once a reader can reach n at any level, all lower
	// levels and n's own fields are already in place.
	for i := 0; i < h; i++ {
		preds[i].next[i].Store(n)
	}

	m.size.Add(int64(len(key)+len(value)) + nodeOverhead + int64(h)*ptrSize)
}

// Get returns the newest entry for userKey visible at seq. ok reports
// whether any version was found; a found KindDelete is a tombstone and is
// the caller's to interpret. The returned value aliases memtable memory and
// must not be modified.
func (m *MemTable) Get(userKey []byte, seq uint64) (value []byte, kind base.Kind, ok bool) {
	seek := base.AppendSeekKey(nil, userKey, seq)
	n := m.findGE(seek, nil)
	if n == nil {
		return nil, 0, false
	}
	ik, err := base.DecodeInternalKey(n.key)
	if err != nil {
		panic("memtable: corrupt internal key in skiplist: " + err.Error())
	}
	if string(ik.UserKey) != string(userKey) {
		return nil, 0, false
	}
	return n.value[:len(n.value):len(n.value)], ik.Kind, true
}

// ApproximateSize returns the approximate memory footprint in bytes; it
// grows monotonically with inserts and drives the engine's flush decision.
func (m *MemTable) ApproximateSize() int64 { return m.size.Load() }

// Iterator walks all entries — every version, tombstones included — in
// base.Compare order. It is valid to use concurrently with the single
// writer: iteration is live, not a snapshot, so entries inserted
// concurrently may or may not be observed — snapshot reads must filter by
// sequence number. Key and Value alias memtable memory, must not be
// modified, and remain valid for the memtable's lifetime, including across
// Next. Next, Key, and Value panic on an invalid iterator.
type Iterator struct {
	m *MemTable
	n *node
}

func (m *MemTable) NewIterator() *Iterator { return &Iterator{m: m} }

// First positions the iterator at the smallest internal key.
func (it *Iterator) First() { it.n = it.m.head.next[0].Load() }

// SeekGE positions the iterator at the first entry whose encoded internal
// key is >= key.
func (it *Iterator) SeekGE(key []byte) { it.n = it.m.findGE(key, nil) }

func (it *Iterator) Next()       { it.n = it.n.next[0].Load() }
func (it *Iterator) Valid() bool { return it.n != nil }

// Error satisfies the engine's internal iterator interface; memtable
// iteration cannot fail.
func (it *Iterator) Error() error { return nil }

// Key and Value are capacity-capped so appends through them cannot reach
// node memory.
func (it *Iterator) Key() []byte   { return it.n.key[:len(it.n.key):len(it.n.key)] }
func (it *Iterator) Value() []byte { return it.n.value[:len(it.n.value):len(it.n.value)] }
