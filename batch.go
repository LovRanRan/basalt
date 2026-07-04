package basalt

import "github.com/LovRanRan/basalt/internal/wal"

// Batch groups writes that commit atomically: the whole batch becomes
// durable as one WAL record (replayed all-or-nothing after a crash), its
// ops receive consecutive sequence numbers, and visibility is published in
// one step — a reader can never observe part of a batch. Key and value
// slices are copied during Apply; the caller may reuse them, and the batch
// itself, afterwards.
type Batch struct {
	inner wal.Batch
}

func (b *Batch) Put(key, value []byte) { b.inner.Put(key, value) }
func (b *Batch) Delete(key []byte)     { b.inner.Delete(key) }
func (b *Batch) Len() int              { return len(b.inner.Ops) }
func (b *Batch) Reset()                { b.inner.Ops = b.inner.Ops[:0] }

// Apply commits the batch atomically; an empty batch is a no-op.
func (db *DB) Apply(b *Batch) error {
	if b.Len() == 0 {
		return nil
	}
	return db.write(&b.inner)
}
