package cluster

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"
)

// testCluster is a 3-node cluster wired over real TCP loopback.
type testCluster struct {
	t     *testing.T
	nodes map[uint64]*Node
	srv   map[uint64]*Servers
	kv    map[uint64]string // id -> kv listen addr
}

func freePort(t *testing.T) (net.Listener, string) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return lis, lis.Addr().String()
}

func startCluster(t *testing.T, ids []uint64) *testCluster {
	t.Helper()
	// Pre-bind raft and kv listeners so peers know each other's addresses.
	raftLis := map[uint64]net.Listener{}
	kvLis := map[uint64]net.Listener{}
	peers := map[uint64]string{}
	kvAddr := map[uint64]string{}
	for _, id := range ids {
		rl, ra := freePort(t)
		kl, ka := freePort(t)
		raftLis[id], peers[id] = rl, ra
		kvLis[id], kvAddr[id] = kl, ka
	}

	tc := &testCluster{t: t, nodes: map[uint64]*Node{}, srv: map[uint64]*Servers{}, kv: kvAddr}
	for _, id := range ids {
		n, err := Open(Config{
			ID: id, Peers: peers, DataDir: filepath.Join(t.TempDir(), fmt.Sprintf("n%d", id)),
			ElectionTick: 10, HeartbeatTick: 1, TickInterval: 10 * time.Millisecond,
		})
		if err != nil {
			t.Fatal(err)
		}
		tc.nodes[id] = n
		tc.srv[id] = n.Serve(raftLis[id], kvLis[id])
	}
	return tc
}

func (tc *testCluster) stop() {
	for id, s := range tc.srv {
		s.Raft.Stop()
		s.KV.Stop()
		_ = tc.nodes[id].Close()
	}
}

func (tc *testCluster) waitLeader(d time.Duration) uint64 {
	tc.t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		for id, n := range tc.nodes {
			if _, _, isLeader := n.Status(); isLeader {
				return id
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	tc.t.Fatal("no leader elected")
	return 0
}

func TestClusterClientRoutesThroughRedirects(t *testing.T) {
	ids := []uint64{1, 2, 3}
	tc := startCluster(t, ids)
	defer tc.stop()
	tc.waitLeader(5 * time.Second)

	c, err := NewClient(tc.kv)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for i := 0; i < 200; i++ {
		if err := c.Put(ctx, []byte(fmt.Sprintf("key-%03d", i)), []byte(fmt.Sprintf("v-%d", i))); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	// Linearizable reads see every prior write, whichever node the client
	// happens to hit first (it redirects to the leader).
	for i := 0; i < 200; i++ {
		v, found, err := c.Get(ctx, []byte(fmt.Sprintf("key-%03d", i)))
		if err != nil || !found || string(v) != fmt.Sprintf("v-%d", i) {
			t.Fatalf("get %d = (%q, %v, %v)", i, v, found, err)
		}
	}
	if _, found, err := c.Get(ctx, []byte("absent")); err != nil || found {
		t.Fatalf("absent: found=%v err=%v", found, err)
	}
}

func TestClusterDirectToFollowerRedirects(t *testing.T) {
	ids := []uint64{1, 2, 3}
	tc := startCluster(t, ids)
	defer tc.stop()
	leader := tc.waitLeader(5 * time.Second)

	// Point a client's cache at a known follower; its first request must
	// redirect and still succeed.
	follower := uint64(1)
	if follower == leader {
		follower = 2
	}
	c, err := NewClient(tc.kv)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.leader = follower // force the first hop to a follower

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c.Put(ctx, []byte("k"), []byte("v")); err != nil {
		t.Fatalf("put via follower: %v", err)
	}
	v, found, err := c.Get(ctx, []byte("k"))
	if err != nil || !found || string(v) != "v" {
		t.Fatalf("get after follower redirect = (%q, %v, %v)", v, found, err)
	}
	// The client learned the leader: it is not the follower we forced.
	if c.leader == follower {
		t.Fatal("client did not update its cached leader after redirect")
	}
}
