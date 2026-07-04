package cluster

import (
	"context"
	"fmt"
	"time"

	basalt "github.com/LovRanRan/basalt"
	"github.com/LovRanRan/basalt/internal/shard"
)

// MigrateSlot moves a slot from src to dst and completes the handoff safely:
// freeze the slot's writes cluster-wide, barrier + final-copy its data into
// dst, flip ownership, then delete the migrated keys from src. It runs on the
// node leading BOTH groups (CopySlot, the src barrier, and source cleanup all
// require it) — co-locate the leaderships with TransferLeader first.
//
// Idempotent: if the slot has already flipped to dst (a prior run got past the
// flip), it re-publishes the flip to any node that missed it and resumes at
// source cleanup. One slot at a time. If a step fails before the flip, the
// freeze is rolled back (best-effort) so an aborted migration never leaves the
// slot's writes frozen.
//
// Single-owner safety across the handoff comes from the freeze, not the
// per-request epoch fence: once the freeze map has reached every live node, no
// node accepts a write for the slot — enforced both at the front door and
// again on the owning group's event loop just before append (ProposeWithGuard),
// so a write that raced the freeze cannot slip in after the barrier. The
// barrier + final copy therefore capture a stable set that dst then owns
// exclusively. The freeze surfaces to clients as a brief retryable
// "slot-migrating" rejection on writes; reads keep flowing from src until the
// flip. Reads are not linearizable across the flip boundary: an in-flight Get
// that routed to src just before the flip may observe NotFound once source
// cleanup deletes the migrated keys — a retry routes to dst and finds the
// value.
func (n *Node) MigrateSlot(ctx context.Context, slot uint32, src, dst uint64) (err error) {
	if slot >= shard.NumSlots {
		return fmt.Errorf("cluster: slot %d out of range", slot)
	}
	if src == dst {
		return fmt.Errorf("cluster: src and dst are the same group (%d)", src)
	}
	sg, dg := n.Group(src), n.Group(dst)
	if sg == nil || dg == nil {
		return fmt.Errorf("cluster: node must host both src group %d and dst group %d", src, dst)
	}
	if _, _, isLeader := sg.Status(); !isLeader {
		return fmt.Errorf("cluster: not the leader of src group %d", src)
	}
	if _, _, isLeader := dg.Status(); !isLeader {
		return fmt.Errorf("cluster: not the leader of dst group %d", dst)
	}

	cur := n.ShardMap()
	if cur.Group(slot) == dst {
		// Ownership already flipped by a prior run: push the flip to any node
		// that missed it, then finish cleanup.
		if perr := n.publishToAll(ctx, cur); perr != nil {
			return fmt.Errorf("cluster: republish flip: %w", perr)
		}
		return n.deleteSlotFrom(ctx, sg, slot)
	}
	if cur.Group(slot) != src {
		return fmt.Errorf("cluster: slot %d is owned by group %d, not src %d", slot, cur.Group(slot), src)
	}

	// Distribution adopts a map on reachable nodes even when it then reports
	// failure (some peer unreachable), so once the freeze publish has been
	// attempted, an aborted migration must unfreeze — otherwise the slot's
	// writes stay rejected for as long as the unrelated outage lasts. Rolled
	// back best-effort on any error before the flip; the original ctx may
	// already be dead, so the rollback gets its own.
	frozen, flipped := false, false
	defer func() {
		if err == nil || !frozen || flipped {
			return
		}
		rctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = n.DistributeShardMap(rctx, n.ShardMap().WithMigrating(slot, false))
	}()

	// 1. Freeze the slot's writes and publish to every live node.
	frozen = true
	if err := n.publishToAll(ctx, cur.WithMigrating(slot, true)); err != nil {
		return fmt.Errorf("cluster: distribute freeze: %w", err)
	}
	// 2. Barrier on src: a committed no-op guarantees every write accepted
	//    before the freeze reached src has committed, so the final copy sees
	//    all of them.
	if err := n.barrier(ctx, sg); err != nil {
		return fmt.Errorf("cluster: src barrier: %w", err)
	}
	// 3. Final copy: the slot is now stable; capture every key into dst.
	if _, _, err := n.CopySlot(ctx, slot, src, dst, nil); err != nil {
		return fmt.Errorf("cluster: final copy: %w", err)
	}
	// 4. Flip ownership to dst (clears the migrating flag) and publish. From
	//    here the flip may be adopted anywhere, so the rollback must not run;
	//    a retry resumes via the already-flipped branch instead.
	flipped = true
	if err := n.publishToAll(ctx, n.ShardMap().WithSlot(slot, dst)); err != nil {
		return fmt.Errorf("cluster: distribute flip: %w", err)
	}
	// 5. Delete the slot's now-dead keys from src.
	if err := n.deleteSlotFrom(ctx, sg, slot); err != nil {
		return fmt.Errorf("cluster: source cleanup: %w", err)
	}
	return nil
}

// publishToAll distributes m and fails unless every live node adopted it (at
// or past its epoch), so a rebalance step never proceeds while some node still
// routes under the old map.
func (n *Node) publishToAll(ctx context.Context, m *shard.ShardMap) error {
	reached, err := n.DistributeShardMap(ctx, m)
	if err != nil {
		return err
	}
	if want := len(n.cfg.Peers); reached < want {
		return fmt.Errorf("map epoch %d reached only %d of %d nodes", m.Epoch, reached, want)
	}
	return nil
}

// barrier proposes a no-op into g and waits for it to commit. Because the raft
// log commits as a prefix, once the no-op applies every earlier proposal has
// too — draining any writes that were in flight when the freeze took effect.
func (n *Node) barrier(ctx context.Context, g *group) error {
	return g.Propose(ctx, &basalt.Command{ClientID: basalt.NoDedupClient | migSeq.Add(1), Seq: 1})
}

// deleteSlotFrom removes every key of slot from g's engine. Run after the flip,
// when the slot is frozen-then-reassigned so no new keys can arrive. Deletes go
// through the no-dedup client namespace and are idempotent (deleting an absent
// key is a no-op), so a retry after a partial cleanup is safe.
func (n *Node) deleteSlotFrom(ctx context.Context, g *group, slot uint32) error {
	client := basalt.NoDedupClient | migSeq.Add(1)
	var seq uint64
	batch := &basalt.Batch{}
	inBatch := 0
	flush := func() error {
		if inBatch == 0 {
			return nil
		}
		seq++
		if err := g.Propose(ctx, &basalt.Command{ClientID: client, Seq: seq, Batch: *batch}); err != nil {
			return err
		}
		batch = &basalt.Batch{}
		inBatch = 0
		return nil
	}

	it := g.DB().Scan(nil, nil)
	defer it.Close()
	for ; it.Valid(); it.Next() {
		if shard.Slot(it.Key()) != slot {
			continue
		}
		batch.Delete(append([]byte(nil), it.Key()...))
		inBatch++
		if inBatch == migrateBatch {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := it.Error(); err != nil {
		return err
	}
	return flush()
}
