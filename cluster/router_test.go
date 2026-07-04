package cluster

import (
	"context"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	basaltv1 "github.com/LovRanRan/basalt/api/basalt/v1"
	"github.com/LovRanRan/basalt/internal/shard"
)

// shardedCluster boots a 3-node/3-group sharded cluster with the routing
// front door and returns per-node client connections.
type shardedCluster struct {
	nodes   map[uint64]*Node
	clients map[uint64]basaltv1.KVServiceClient
	smap    *shard.ShardMap
}

func startSharded(t *testing.T, groups []uint64) *shardedCluster {
	t.Helper()
	ids := []uint64{1, 2, 3}
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
	smap := shard.NewShardMap(groups)
	sc := &shardedCluster{nodes: map[uint64]*Node{}, clients: map[uint64]basaltv1.KVServiceClient{}, smap: smap}
	for _, id := range ids {
		n, err := Open(Config{
			ID: id, Peers: peers, DataDir: filepath.Join(t.TempDir(), fmt.Sprintf("n%d", id)),
			ElectionTick: 10, HeartbeatTick: 1, TickInterval: 10 * time.Millisecond, Groups: groups,
		})
		if err != nil {
			t.Fatal(err)
		}
		sc.nodes[id] = n
		srv := n.ServeSharded(raftLis[id], kvLis[id], smap)
		t.Cleanup(func() { srv.Raft.Stop(); srv.KV.Stop() })
	}
	for _, id := range ids {
		conn, err := grpc.NewClient(kvAddr[id], grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatal(err)
		}
		sc.clients[id] = basaltv1.NewKVServiceClient(conn)
		t.Cleanup(func() { _ = conn.Close() })
	}
	t.Cleanup(func() {
		for _, n := range sc.nodes {
			_ = n.Close()
		}
	})
	return sc
}

func (sc *shardedCluster) waitAllGroupsLed(t *testing.T, groups []uint64) {
	t.Helper()
	for _, gid := range groups {
		waitGroupLeader(t, sc.nodes, gid, 20*time.Second)
	}
}

func TestRouterAnyNodeServesAllOps(t *testing.T) {
	groups := []uint64{10, 20, 30}
	sc := startSharded(t, groups)
	sc.waitAllGroupsLed(t, groups)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Writes go through a DIFFERENT node each time; the front door forwards
	// each to whichever node leads the owning group.
	const n = 300
	for i := 0; i < n; i++ {
		node := uint64(i%3 + 1)
		_, err := sc.clients[node].Put(ctx, &basaltv1.PutRequest{
			Key: []byte(fmt.Sprintf("key-%04d", i)), Value: []byte(fmt.Sprintf("v-%d", i)),
		})
		if err != nil {
			t.Fatalf("put %d via node %d: %v", i, node, err)
		}
	}
	// Reads from yet another node see every write, wherever it landed.
	for i := 0; i < n; i++ {
		node := uint64((i+1)%3 + 1)
		resp, err := sc.clients[node].Get(ctx, &basaltv1.GetRequest{Key: []byte(fmt.Sprintf("key-%04d", i))})
		if err != nil || !resp.GetFound() || string(resp.GetValue()) != fmt.Sprintf("v-%d", i) {
			t.Fatalf("get %d via node %d = (%q, %v, %v)", i, node, resp.GetValue(), resp.GetFound(), err)
		}
	}
	// Delete via a third node, verify gone.
	if _, err := sc.clients[3].Delete(ctx, &basaltv1.DeleteRequest{Key: []byte("key-0100")}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if resp, err := sc.clients[1].Get(ctx, &basaltv1.GetRequest{Key: []byte("key-0100")}); err != nil || resp.GetFound() {
		t.Fatalf("deleted key still present via node 1: %v %v", resp.GetFound(), err)
	}
}

func TestRouterScatterGatherScanIsGloballyOrdered(t *testing.T) {
	groups := []uint64{10, 20, 30}
	sc := startSharded(t, groups)
	sc.waitAllGroupsLed(t, groups)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Write keys that fan out across all three groups.
	const n = 240
	want := map[string]string{}
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("key-%04d", i)
		v := fmt.Sprintf("v-%d", i)
		if _, err := sc.clients[uint64(i%3+1)].Put(ctx, &basaltv1.PutRequest{Key: []byte(k), Value: []byte(v)}); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
		want[k] = v
	}
	// Confirm the keys really did land in more than one group (otherwise the
	// scatter-gather is not exercised).
	usedGroups := map[uint64]bool{}
	for k := range want {
		usedGroups[sc.smap.Lookup([]byte(k))] = true
	}
	if len(usedGroups) < 2 {
		t.Fatalf("keys only used %d group(s); scatter-gather not exercised", len(usedGroups))
	}

	// A full scan through ANY node returns every key, globally sorted, even
	// though each group holds a hash-scattered subset.
	collect := func(node uint64, req *basaltv1.ScanRequest) []*basaltv1.KeyValue {
		t.Helper()
		stream, err := sc.clients[node].Scan(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		var got []*basaltv1.KeyValue
		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, resp.GetPairs()...)
		}
		return got
	}

	got := collect(2, &basaltv1.ScanRequest{})
	if len(got) != n {
		t.Fatalf("full scan returned %d pairs, want %d", len(got), n)
	}
	// Globally ordered and complete.
	for i := 1; i < len(got); i++ {
		if string(got[i-1].GetKey()) >= string(got[i].GetKey()) {
			t.Fatalf("scan out of order at %d: %q then %q", i, got[i-1].GetKey(), got[i].GetKey())
		}
	}
	for _, kv := range got {
		if want[string(kv.GetKey())] != string(kv.GetValue()) {
			t.Fatalf("scan value mismatch for %q", kv.GetKey())
		}
	}

	// Bounded + limited scan is also globally ordered.
	bounded := collect(1, &basaltv1.ScanRequest{Start: []byte("key-0050"), End: []byte("key-0150"), Limit: 30})
	if len(bounded) != 30 {
		t.Fatalf("bounded+limited scan = %d, want 30", len(bounded))
	}
	var wantKeys []string
	for k := range want {
		if k >= "key-0050" && k < "key-0150" {
			wantKeys = append(wantKeys, k)
		}
	}
	sort.Strings(wantKeys)
	for i, kv := range bounded {
		if string(kv.GetKey()) != wantKeys[i] {
			t.Fatalf("bounded scan[%d] = %q, want %q", i, kv.GetKey(), wantKeys[i])
		}
	}
}
