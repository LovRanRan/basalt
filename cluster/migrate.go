package cluster

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"

	basalt "github.com/LovRanRan/basalt"
	"github.com/LovRanRan/basalt/internal/shard"
)

// migrateBatch is how many keys one migration command carries into dst.
const migrateBatch = 512

// migSeq hands each CopySlot invocation a unique id, used both to name its
// scratch checkpoint and to keep proposal waiter keys distinct across
// concurrent copies. Migration writes go through the engine's no-dedup client
// namespace (basalt.NoDedupClient), so this counter resetting on restart is
// harmless: no dst session is recorded, and copies are idempotent by value.
var migSeq atomic.Uint64

// CopySlot streams every user key in slot from the src group to the dst group.
// It takes a linearizable read barrier on src, checkpoints src's engine for a
// stable point-in-time snapshot, scans the snapshot for the slot's keys, and
// proposes them into dst's raft log in batches. This is the data-movement half
// of a rebalance; it does NOT change ownership — the shard map still routes the
// slot to src. The map flip, fencing the write tail, and deleting the migrated
// keys from src are P4.8.
//
// The barrier guarantees the snapshot contains every write acknowledged before
// the call, so a rebalance can pre-copy in bulk here and later re-run under a
// write freeze (P4.8) to capture the tail. It therefore requires this node to
// LEAD BOTH src (for the barrier) and dst (to propose into it): a coordinator
// co-locates the two leaderships with TransferLeader before migrating a slot,
// which keeps the whole copy node-local and consistent.
//
// The engine's Scan already excludes reserved (raft-internal) keys, so only
// user data is copied. Idempotent: re-proposing a key overwrites it with the
// same value. Resumable: it returns the last key copied; pass it back as after
// to continue where a previous call stopped.
func (n *Node) CopySlot(ctx context.Context, slot uint32, src, dst uint64, after []byte) (lastKey []byte, copied int, err error) {
	if slot >= shard.NumSlots {
		return nil, 0, fmt.Errorf("cluster: slot %d out of range", slot)
	}
	if src == dst {
		return nil, 0, fmt.Errorf("cluster: src and dst are the same group (%d)", src)
	}
	sg, dg := n.Group(src), n.Group(dst)
	if sg == nil {
		return nil, 0, fmt.Errorf("cluster: src group %d not hosted here", src)
	}
	if dg == nil {
		return nil, 0, fmt.Errorf("cluster: dst group %d not hosted here", dst)
	}
	if l, _, isLeader := dg.Status(); !isLeader {
		return nil, 0, &ErrNotLeader{Leader: l}
	}
	// Read barrier on src: guarantees this (leader) replica has applied every
	// write committed before now, so the checkpoint below is current. Also
	// enforces src leadership (ReadIndex is leader-only).
	if rerr := sg.ReadIndex(ctx); rerr != nil {
		return nil, 0, rerr
	}

	id := migSeq.Add(1)

	// Checkpoint src into a scratch dir beside its engine (same filesystem, so
	// tables are hard-linked, not copied), open it as a stable read snapshot,
	// and remove it when done.
	ckpt := filepath.Join(n.groupDir(src), fmt.Sprintf("migrate-slot%d-%d", slot, id))
	if err := sg.DB().Checkpoint(ckpt); err != nil {
		return nil, 0, fmt.Errorf("cluster: checkpoint src group %d: %w", src, err)
	}
	defer func() { _ = os.RemoveAll(ckpt) }()
	// Read-only, scan-once view: no WAL, and no background compaction of
	// tables we are about to discard.
	snap, err := basalt.Open(ckpt, basalt.Options{DisableWAL: true, DisableCompaction: true})
	if err != nil {
		return nil, 0, fmt.Errorf("cluster: open src checkpoint: %w", err)
	}
	defer func() { _ = snap.Close() }()

	// No-dedup client namespace, low bits unique per call so concurrent copies
	// never share a waiter key.
	client := basalt.NoDedupClient | id
	var seq uint64
	batch := &basalt.Batch{}
	inBatch := 0
	var pendingLast []byte // last key accumulated but not yet committed
	propose := func() error {
		if inBatch == 0 {
			return nil
		}
		seq++
		if perr := dg.Propose(ctx, &basalt.Command{ClientID: client, Seq: seq, Batch: *batch}); perr != nil {
			return fmt.Errorf("cluster: propose into dst group %d: %w", dst, perr)
		}
		// Only now are these keys durable in dst: advance the resume cursor
		// and the committed count. Returning a cursor past uncommitted keys
		// would let a resume skip them.
		copied += inBatch
		lastKey = pendingLast
		batch = &basalt.Batch{}
		inBatch = 0
		return nil
	}

	// Resume strictly after `after`: seek to the smallest key greater than it.
	start := after
	if len(after) > 0 {
		start = append(append([]byte(nil), after...), 0x00)
	}
	it := snap.Scan(start, nil)
	defer it.Close()

	for ; it.Valid(); it.Next() {
		if shard.Slot(it.Key()) != slot {
			continue
		}
		key := append([]byte(nil), it.Key()...)
		batch.Put(key, append([]byte(nil), it.Value()...))
		inBatch++
		pendingLast = key
		if inBatch == migrateBatch {
			if perr := propose(); perr != nil {
				return lastKey, copied, perr
			}
		}
	}
	if serr := it.Error(); serr != nil {
		return lastKey, copied, fmt.Errorf("cluster: scan src checkpoint: %w", serr)
	}
	if perr := propose(); perr != nil {
		return lastKey, copied, perr
	}
	return lastKey, copied, nil
}
