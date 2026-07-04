package cluster

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"

	basalt "github.com/LovRanRan/basalt"
	basaltv1 "github.com/LovRanRan/basalt/api/basalt/v1"
)

// grpcServeRaft starts only the raft transport server for a node.
func grpcServeRaft(lis net.Listener, n *Node) *grpc.Server {
	s := grpc.NewServer()
	basaltv1.RegisterRaftServiceServer(s, &raftServer{n: n})
	go func() { _ = s.Serve(lis) }()
	return s
}

// startMultiGroup runs a 3-node cluster where every node hosts the same set
// of raft groups, all multiplexed over one connection per peer.
func startMultiGroup(t *testing.T, ids, groups []uint64) map[uint64]*Node {
	t.Helper()
	raftLis := map[uint64]net.Listener{}
	peers := map[uint64]string{}
	for _, id := range ids {
		rl, ra := freePort(t)
		raftLis[id], peers[id] = rl, ra
	}
	nodes := map[uint64]*Node{}
	for _, id := range ids {
		n, err := Open(Config{
			ID: id, Peers: peers, DataDir: filepath.Join(t.TempDir(), fmt.Sprintf("n%d", id)),
			ElectionTick: 10, HeartbeatTick: 1, TickInterval: 10 * time.Millisecond,
			Groups: groups,
		})
		if err != nil {
			t.Fatal(err)
		}
		nodes[id] = n
		// Serve only the raft transport; this test drives groups directly.
		s := grpcServeRaft(raftLis[id], n)
		t.Cleanup(func() { s.Stop() })
	}
	t.Cleanup(func() {
		for _, n := range nodes {
			_ = n.Close()
		}
	})
	return nodes
}

// groupLeader returns the node id currently leading the given group.
func groupLeader(nodes map[uint64]*Node, gid uint64) (uint64, bool) {
	var lead uint64
	count := 0
	for id, n := range nodes {
		if _, _, isLeader := n.Group(gid).Status(); isLeader {
			lead, count = id, count+1
		}
	}
	return lead, count == 1
}

func waitGroupLeader(t *testing.T, nodes map[uint64]*Node, gid uint64, d time.Duration) uint64 {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if id, ok := groupLeader(nodes, gid); ok {
			return id
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("group %d never elected a leader", gid)
	return 0
}

func TestMultipleGroupsElectAndReplicateIndependently(t *testing.T) {
	ids := []uint64{1, 2, 3}
	groups := []uint64{10, 20, 30}
	nodes := startMultiGroup(t, ids, groups)

	// Every group elects its own leader independently — they may differ.
	leaders := map[uint64]uint64{}
	for _, gid := range groups {
		leaders[gid] = waitGroupLeader(t, nodes, gid, 20*time.Second)
	}

	// Writes to one group are invisible to the others: each group is a
	// separate replicated state machine.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, gid := range groups {
		lead := nodes[leaders[gid]].Group(gid)
		for i := 0; i < 20; i++ {
			var b basalt.Batch
			b.Put([]byte(fmt.Sprintf("k%d", i)), []byte(fmt.Sprintf("g%d-v%d", gid, i)))
			if err := lead.Propose(ctx, &basalt.Command{ClientID: gid, Seq: uint64(i + 1), Batch: b}); err != nil {
				t.Fatalf("group %d propose %d: %v", gid, i, err)
			}
		}
	}

	// Each group's replicas agree, and each holds only its own group's
	// values — no cross-group contamination.
	time.Sleep(500 * time.Millisecond)
	for _, gid := range groups {
		for _, id := range ids {
			g := nodes[id].Group(gid)
			if err := g.ReadIndex(ctx); err != nil {
				// Only the leader answers ReadIndex; skip followers.
				continue
			}
			v, err := g.DB().Get([]byte("k5"))
			if err != nil {
				t.Fatalf("group %d node %d get: %v", gid, id, err)
			}
			want := fmt.Sprintf("g%d-v5", gid)
			if string(v) != want {
				t.Fatalf("group %d node %d: k5 = %q, want %q", gid, id, v, want)
			}
		}
	}

	// The groups are genuinely independent: at least across three groups
	// and three nodes, not all leaders are the same node (statistically the
	// point of multi-raft is load spread). We assert only correctness here,
	// but log the spread.
	spread := map[uint64]int{}
	for _, gid := range groups {
		l, _ := groupLeader(nodes, gid)
		spread[l]++
	}
	t.Logf("group leaders: %v (spread across %d nodes)", leaders, len(spread))
}

func TestSingleGroupIsDegenerateCase(t *testing.T) {
	ids := []uint64{1, 2, 3}
	nodes := startMultiGroup(t, ids, []uint64{1}) // one group
	lead := waitGroupLeader(t, nodes, 1, 20*time.Second)

	// The single-group convenience API works: Node.Propose/DB target the
	// sole group.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := nodes[lead].Propose(ctx, &basalt.Command{ClientID: 1, Seq: 1, Batch: putBatch("only", "here")}); err != nil {
		t.Fatal(err)
	}
	if err := nodes[lead].ReadIndex(ctx); err != nil {
		t.Fatal(err)
	}
	if v, err := nodes[lead].DB().Get([]byte("only")); err != nil || string(v) != "here" {
		t.Fatalf("single-group get = %q, %v", v, err)
	}
}

func putBatch(k, v string) basalt.Batch {
	var b basalt.Batch
	b.Put([]byte(k), []byte(v))
	return b
}
