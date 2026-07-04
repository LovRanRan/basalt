package sstable

import (
	"fmt"

	"github.com/LovRanRan/basalt/internal/base"
)

// Iterator walks a whole table in key order, loading data blocks lazily.
// Key and Value are only valid until the next positioning call (First,
// SeekGE, Next); copy them to retain. After any corruption or I/O failure
// the iterator becomes invalid, Error reports the cause, and further
// positioning calls are silent no-ops — a consumer must check Error
// whenever the iterator goes invalid to distinguish exhaustion from
// failure. Not safe for concurrent use; create one per goroutine.
type Iterator struct {
	r        *Reader
	blockIdx int
	bi       *blockIter
	err      error
}

func (r *Reader) NewIterator() *Iterator {
	return &Iterator{r: r, blockIdx: -1}
}

func (it *Iterator) fail(err error) {
	if it.err == nil {
		it.err = err
	}
	it.bi = nil
}

// loadBlock opens data block i and returns its iterator, or fails.
func (it *Iterator) loadBlock(i int) *blockIter {
	region := fmt.Sprintf("data[%d]", i)
	contents, err := it.r.readBlock(it.r.index[i].handle, region)
	if err != nil {
		it.fail(err)
		return nil
	}
	pb, err := parseBlock(contents, region)
	if err != nil {
		it.fail(err)
		return nil
	}
	it.blockIdx = i
	return newBlockIter(pb, region)
}

func (it *Iterator) First() {
	if it.err != nil {
		return
	}
	it.bi = nil
	if len(it.r.index) == 0 {
		return
	}
	if bi := it.loadBlock(0); bi != nil {
		bi.first()
		it.settle(bi)
	}
}

// SeekGE positions at the first entry whose encoded internal key is >= key.
func (it *Iterator) SeekGE(key []byte) {
	if it.err != nil {
		return
	}
	it.bi = nil
	i := it.r.seekIndex(key)
	if i == len(it.r.index) {
		return
	}
	if bi := it.loadBlock(i); bi != nil {
		bi.seekGE(key)
		it.settle(bi)
		// The index promised this block holds a key >= the target; landing
		// below it means the index lies about the block's contents.
		if it.Valid() && base.Compare(it.bi.key, key) < 0 {
			it.fail(corruptf("data[%d]: index key out of sync with block contents", it.blockIdx))
		}
	}
}

// Next advances; it panics on an invalid iterator, matching the memtable
// iterator's contract.
func (it *Iterator) Next() {
	if it.bi == nil || !it.bi.valid {
		panic("sstable: Next on invalid iterator")
	}
	it.bi.next()
	it.settle(it.bi)
}

// settle resolves a block iterator that ran off its block's end by moving
// to the next block; the writer never emits empty data blocks, so one step
// suffices.
func (it *Iterator) settle(bi *blockIter) {
	if bi.err != nil {
		it.fail(bi.err)
		return
	}
	if bi.valid {
		it.bi = bi
		return
	}
	if it.blockIdx+1 >= len(it.r.index) {
		it.bi = nil
		return
	}
	if next := it.loadBlock(it.blockIdx + 1); next != nil {
		next.first()
		if next.err != nil {
			it.fail(next.err)
			return
		}
		if !next.valid {
			it.fail(corruptf("data[%d]: unexpectedly empty block", it.blockIdx))
			return
		}
		it.bi = next
	}
}

func (it *Iterator) Valid() bool   { return it.err == nil && it.bi != nil && it.bi.valid }
func (it *Iterator) Error() error  { return it.err }
func (it *Iterator) Key() []byte   { return it.bi.key }
func (it *Iterator) Value() []byte { return it.bi.value }
