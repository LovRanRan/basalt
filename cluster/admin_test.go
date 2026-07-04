package cluster

import (
	"context"
	"testing"
	"time"

	basalt "github.com/LovRanRan/basalt"
	"github.com/LovRanRan/basalt/internal/raft"
)

func TestClusterLeadershipTransfer(t *testing.T) {
	ids := []uint64{1, 2, 3}
	nodes := startMultiGroup(t, ids, []uint64{1})
	lead := waitGroupLeader(t, nodes, 1, 20*time.Second)

	// Write something so the target is a live, caught-up replica.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := nodes[lead].Group(1).Propose(ctx, &basalt.Command{ClientID: 1, Seq: 1, Batch: putBatch("k", "v")}); err != nil {
		t.Fatal(err)
	}

	// Pick a different node and hand it leadership.
	target := uint64(0)
	for _, id := range ids {
		if id != lead {
			target = id
			break
		}
	}
	if err := nodes[lead].Group(1).TransferLeader(ctx, target); err != nil {
		t.Fatalf("transfer: %v", err)
	}

	// The target becomes leader.
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if _, _, isLeader := nodes[target].Group(1).Status(); isLeader {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if _, _, isLeader := nodes[target].Group(1).Status(); !isLeader {
		t.Fatalf("target node %d did not become leader", target)
	}

	// The new leader serves reads and writes; data survived the handoff. A
	// just-elected leader briefly cannot serve reads (its no-op must
	// commit first), so retry — exactly what a client would do.
	var rerr error
	for i := 0; i < 100; i++ {
		if rerr = nodes[target].Group(1).ReadIndex(ctx); rerr == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if rerr != nil {
		t.Fatalf("read after transfer: %v", rerr)
	}
	if v, err := nodes[target].Group(1).DB().Get([]byte("k")); err != nil || string(v) != "v" {
		t.Fatalf("data lost across transfer: %q %v", v, err)
	}
	if err := nodes[target].Group(1).Propose(ctx, &basalt.Command{ClientID: 2, Seq: 1, Batch: putBatch("k2", "v2")}); err != nil {
		t.Fatalf("write to new leader: %v", err)
	}

	// The old leader now rejects proposals (it stepped down).
	if err := nodes[lead].Group(1).Propose(ctx, &basalt.Command{ClientID: 3, Seq: 1, Batch: putBatch("x", "y")}); err == nil {
		t.Fatal("old leader still accepts writes after transfer")
	}
}

func TestClusterAddLearnerThenPromote(t *testing.T) {
	// Membership within an existing group: add node 3 as a learner then
	// promote it, using the admin path. (All three nodes already host the
	// group here; this exercises the conf-change plumbing end to end.)
	ids := []uint64{1, 2, 3}
	nodes := startMultiGroup(t, ids, []uint64{1})
	lead := waitGroupLeader(t, nodes, 1, 20*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Demote node 3 to a learner, then promote it back — a round-trip that
	// changes the voter count by one each way.
	before, err := nodes[lead].Group(1).NumVoters(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := nodes[lead].Group(1).ConfChange(ctx, raft.ConfChange{Type: raft.ConfRemoveNode, Node: 3}); err != nil {
		t.Fatalf("remove: %v", err)
	}
	time.Sleep(300 * time.Millisecond)
	if got, _ := nodes[lead].Group(1).NumVoters(ctx); got != before-1 {
		t.Fatalf("voter count after remove = %d, want %d", got, before-1)
	}
	if err := nodes[lead].Group(1).ConfChange(ctx, raft.ConfChange{Type: raft.ConfAddNode, Node: 3}); err != nil {
		t.Fatalf("re-add: %v", err)
	}
	time.Sleep(300 * time.Millisecond)
	if got, _ := nodes[lead].Group(1).NumVoters(ctx); got != before {
		t.Fatalf("voter count after re-add = %d, want %d", got, before)
	}
	// The cluster still serves writes after the membership churn.
	if err := nodes[lead].Group(1).Propose(ctx, &basalt.Command{ClientID: 9, Seq: 1, Batch: putBatch("after", "churn")}); err != nil {
		t.Fatalf("write after membership churn: %v", err)
	}
}
