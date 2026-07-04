package basalt

import (
	"container/heap"

	"github.com/LovRanRan/basalt/internal/base"
)

// internalIterator is the shape shared by memtable and sstable iterators:
// positioning over encoded internal keys, with Error distinguishing
// exhaustion from failure (memtable iterators never fail).
type internalIterator interface {
	First()
	SeekGE(key []byte)
	Next()
	Valid() bool
	Key() []byte
	Value() []byte
	Error() error
}

// iterHeap is a min-heap of valid iterators ordered by current internal
// key. Internal keys are unique across all sources — the engine never
// replays a flushed batch — so ties cannot occur.
type iterHeap []internalIterator

func (h iterHeap) Len() int           { return len(h) }
func (h iterHeap) Less(i, j int) bool { return base.Compare(h[i].Key(), h[j].Key()) < 0 }
func (h iterHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *iterHeap) Push(x any)        { *h = append(*h, x.(internalIterator)) }
func (h *iterHeap) Pop() any          { old := *h; x := old[len(old)-1]; *h = old[:len(old)-1]; return x }

// mergingIter yields the union of its children in internal-key order. A
// child error is sticky and makes the merge invalid; the consumer must
// check Error whenever the merge goes invalid.
type mergingIter struct {
	children []internalIterator
	h        iterHeap
	err      error
}

func newMergingIter(children []internalIterator) *mergingIter {
	return &mergingIter{children: children}
}

func (m *mergingIter) fail(err error) {
	if m.err == nil {
		m.err = err
	}
	m.h = m.h[:0]
}

// reposition applies f to every child and rebuilds the heap.
func (m *mergingIter) reposition(f func(internalIterator)) {
	if m.err != nil {
		return
	}
	m.h = m.h[:0]
	for _, c := range m.children {
		f(c)
		if err := c.Error(); err != nil {
			m.fail(err)
			return
		}
		if c.Valid() {
			m.h = append(m.h, c)
		}
	}
	heap.Init(&m.h)
}

func (m *mergingIter) First()            { m.reposition(func(c internalIterator) { c.First() }) }
func (m *mergingIter) SeekGE(key []byte) { m.reposition(func(c internalIterator) { c.SeekGE(key) }) }

func (m *mergingIter) Next() {
	if !m.Valid() {
		panic("basalt: Next on invalid merging iterator")
	}
	top := m.h[0]
	top.Next()
	if err := top.Error(); err != nil {
		m.fail(err)
		return
	}
	if top.Valid() {
		heap.Fix(&m.h, 0)
	} else {
		heap.Pop(&m.h)
	}
}

func (m *mergingIter) Valid() bool   { return m.err == nil && len(m.h) > 0 }
func (m *mergingIter) Key() []byte   { return m.h[0].Key() }
func (m *mergingIter) Value() []byte { return m.h[0].Value() }
func (m *mergingIter) Error() error  { return m.err }
