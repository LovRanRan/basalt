package cluster

import (
	"context"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	basaltv1 "github.com/LovRanRan/basalt/api/basalt/v1"
	"github.com/LovRanRan/basalt/internal/shard"
)

// transientRoute reports a retryable routing error — a group momentarily
// leaderless (election in progress) or a stale-map redirect. A real client
// (cluster.Client) retries these; the tests drive raw per-node stubs, so they
// retry explicitly rather than fail on a transient election.
func transientRoute(err error) bool {
	st, ok := status.FromError(err)
	if !ok {
		return false
	}
	switch st.Code() {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Aborted:
		return true
	case codes.FailedPrecondition:
		return strings.HasPrefix(st.Message(), "not-leader:") || strings.HasPrefix(st.Message(), migratingPrefix)
	default:
		return false
	}
}

// retryRPC runs fn until it succeeds or a transient error stops being
// transient (or the budget is spent), mirroring cluster.Client's retry loop.
func retryRPC(t *testing.T, ctx context.Context, fn func() error) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		err := fn()
		if err == nil {
			return
		}
		if !transientRoute(err) || time.Now().After(deadline) {
			t.Fatalf("rpc: %v", err)
		}
		select {
		case <-time.After(20 * time.Millisecond):
		case <-ctx.Done():
			t.Fatalf("rpc: %v", ctx.Err())
		}
	}
}

func (sc *shardedCluster) mustPut(t *testing.T, ctx context.Context, node uint64, k, v string) {
	t.Helper()
	retryRPC(t, ctx, func() error {
		_, err := sc.clients[node].Put(ctx, &basaltv1.PutRequest{Key: []byte(k), Value: []byte(v)})
		return err
	})
}

func (sc *shardedCluster) mustGet(t *testing.T, ctx context.Context, node uint64, k string) *basaltv1.GetResponse {
	t.Helper()
	var resp *basaltv1.GetResponse
	retryRPC(t, ctx, func() error {
		r, err := sc.clients[node].Get(ctx, &basaltv1.GetRequest{Key: []byte(k)})
		if err != nil {
			return err
		}
		resp = r
		return nil
	})
	return resp
}

func (sc *shardedCluster) mustDelete(t *testing.T, ctx context.Context, node uint64, k string) {
	t.Helper()
	retryRPC(t, ctx, func() error {
		_, err := sc.clients[node].Delete(ctx, &basaltv1.DeleteRequest{Key: []byte(k)})
		return err
	})
}

// shardedCluster boots a 3-node/3-group sharded cluster with the routing
// front door and returns per-node client connections.
type shardedCluster struct {
	nodes   map[uint64]*Node
	clients map[uint64]basaltv1.KVServiceClient
	srvs    map[uint64]*Servers
	kvAddrs map[uint64]string
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
	sc := &shardedCluster{
		nodes: map[uint64]*Node{}, clients: map[uint64]basaltv1.KVServiceClient{},
		srvs: map[uint64]*Servers{}, kvAddrs: kvAddr, smap: smap,
	}
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
		sc.srvs[id] = srv
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
		sc.mustPut(t, ctx, uint64(i%3+1), fmt.Sprintf("key-%04d", i), fmt.Sprintf("v-%d", i))
	}
	// Reads from yet another node see every write, wherever it landed.
	for i := 0; i < n; i++ {
		node := uint64((i+1)%3 + 1)
		resp := sc.mustGet(t, ctx, node, fmt.Sprintf("key-%04d", i))
		if !resp.GetFound() || string(resp.GetValue()) != fmt.Sprintf("v-%d", i) {
			t.Fatalf("get %d via node %d = (%q, %v)", i, node, resp.GetValue(), resp.GetFound())
		}
	}
	// Delete via a third node, verify gone.
	sc.mustDelete(t, ctx, 3, "key-0100")
	if resp := sc.mustGet(t, ctx, 1, "key-0100"); resp.GetFound() {
		t.Fatal("deleted key still present via node 1")
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
		sc.mustPut(t, ctx, uint64(i%3+1), k, v)
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
		var got []*basaltv1.KeyValue
		retryRPC(t, ctx, func() error {
			stream, err := sc.clients[node].Scan(ctx, req)
			if err != nil {
				return err
			}
			var acc []*basaltv1.KeyValue
			for {
				resp, err := stream.Recv()
				if err == io.EOF {
					break
				}
				if err != nil {
					return err
				}
				acc = append(acc, resp.GetPairs()...)
			}
			got = acc
			return nil
		})
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
