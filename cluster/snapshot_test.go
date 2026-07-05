package cluster

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	basalt "github.com/LovRanRan/basalt"
)

// TestSnapshotInstallOverRPC proves a follower that fell behind a compacted
// log rejoins through the streamed snapshot install: node 3 goes unreachable,
// the leader appends and compacts far past it, and on reconnect the leader
// ships a checkpoint that node 3 installs before replicating the tail
// normally.
func TestSnapshotInstallOverRPC(t *testing.T) {
	ids := []uint64{1, 2, 3}
	raftLis := map[uint64]net.Listener{}
	addrs := map[uint64]string{}
	for _, id := range ids {
		rl, ra := freePort(t)
		raftLis[id], addrs[id] = rl, ra
	}
	nodes := map[uint64]*Node{}
	for _, id := range ids {
		n, err := Open(Config{
			ID: id, Peers: addrs, DataDir: filepath.Join(t.TempDir(), fmt.Sprintf("n%d", id)),
			ElectionTick: 10, HeartbeatTick: 1, TickInterval: 10 * time.Millisecond,
			SnapshotEvery: 40, // compact aggressively so the gap opens fast
		})
		if err != nil {
			t.Fatal(err)
		}
		nodes[id] = n
	}
	t.Cleanup(func() {
		for _, n := range nodes {
			_ = n.Close()
		}
	})
	srvs := map[uint64]interface{ Stop() }{}
	for _, id := range ids {
		srvs[id] = grpcServeRaft(raftLis[id], nodes[id])
	}
	t.Cleanup(func() {
		for _, s := range srvs {
			s.Stop()
		}
	})

	lead := waitGroupLeader(t, nodes, 1, 40*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	if lead == 3 {
		if err := nodes[3].Group(1).TransferLeader(ctx, 1); err != nil {
			t.Fatalf("transfer off node 3: %v", err)
		}
		deadline := time.Now().Add(30 * time.Second)
		for time.Now().Before(deadline) {
			if l, ok := groupLeader(nodes, 1); ok && l != 3 {
				lead = l
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		if lead == 3 {
			t.Fatal("leadership never moved off node 3")
		}
	}

	// Node 3 goes dark: its raft server stops, so it receives nothing.
	srvs[3].Stop()

	// Drive the log far past the compaction threshold. Retry each propose:
	// leadership may shuffle among the two live nodes under load.
	propose := func(i int) {
		t.Helper()
		key, val := fmt.Sprintf("snap-%03d", i), fmt.Sprintf("v%d", i)
		deadline := time.Now().Add(20 * time.Second)
		for {
			var b basalt.Batch
			b.Put([]byte(key), []byte(val))
			pctx, pcancel := context.WithTimeout(ctx, 2*time.Second)
			err := nodes[lead].Group(1).Propose(pctx, &basalt.Command{ClientID: basalt.NoDedupClient | 0x51, Seq: uint64(i + 1), Batch: b})
			pcancel()
			if err == nil {
				return
			}
			if l, ok := groupLeader(nodes, 1); ok {
				lead = l
			}
			if time.Now().After(deadline) {
				t.Fatalf("propose %d: %v", i, err)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	for i := 0; i < 200; i++ {
		propose(i)
	}
	// The leader must actually have compacted past node 3's position, or the
	// test would pass without exercising the snapshot path at all.
	deadline := time.Now().Add(30 * time.Second)
	for nodes[lead].Group(1).raft.SnapIndex() == 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	snapIdx := nodes[lead].Group(1).raft.SnapIndex()
	if snapIdx == 0 {
		t.Fatal("leader never compacted its log; snapshot path not exercised")
	}
	if applied := nodes[3].Group(1).sm.AppliedIndex(); applied >= snapIdx {
		t.Fatalf("node 3 (applied %d) not behind the compaction boundary %d", applied, snapIdx)
	}

	// Node 3 comes back on the same address: the leader's next append probe
	// finds the gap and streams the snapshot.
	rl, err := net.Listen("tcp", addrs[3])
	if err != nil {
		t.Fatalf("rebind node 3: %v", err)
	}
	srvs[3] = grpcServeRaft(rl, nodes[3])

	// Node 3 converges: it holds both an early (pre-compaction) key and the
	// final key, and its applied index reached at least the boundary. The
	// install replaces the group object, so re-fetch it every poll.
	waitFor := func(key, val string) {
		t.Helper()
		deadline := time.Now().Add(60 * time.Second)
		for time.Now().Before(deadline) {
			if g := nodes[3].Group(1); g != nil {
				if v, err := g.DB().Get([]byte(key)); err == nil && string(v) == val {
					return
				}
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatalf("node 3 never converged to %s=%s after snapshot install", key, val)
	}
	waitFor("snap-000", "v0")
	waitFor("snap-199", "v199")
	if applied := nodes[3].Group(1).sm.AppliedIndex(); applied < snapIdx {
		t.Fatalf("node 3 applied %d, below the snapshot boundary %d", applied, snapIdx)
	}
}
