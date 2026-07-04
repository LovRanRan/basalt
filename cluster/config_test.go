package cluster

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	basaltv1 "github.com/LovRanRan/basalt/api/basalt/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestConfigValidationFieldErrors(t *testing.T) {
	cases := []struct {
		name string
		yaml string
		want string
	}{
		{"no groups", "nodes:\n  - {id: 1, raft: a, client: b}\n", "groups"},
		{"zero group", "groups: [0]\nnodes: [{id: 1, raft: a, client: b}]\n", "groups[0]"},
		{"dup group", "groups: [5, 5]\nnodes: [{id: 1, raft: a, client: b}]\n", "duplicate group id"},
		{"no nodes", "groups: [1]\n", "nodes must list"},
		{"even nodes", "groups: [1]\nnodes: [{id: 1, raft: a, client: b}, {id: 2, raft: c, client: d}]\n", "even cluster size"},
		{"zero id", "groups: [1]\nnodes: [{id: 0, raft: a, client: b}]\n", "nodes[0].id"},
		{"dup id", "groups: [1]\nnodes: [{id: 1, raft: a, client: b}, {id: 1, raft: c, client: d}, {id: 3, raft: e, client: f}]\n", "duplicate node id"},
		{"missing raft", "groups: [1]\nnodes: [{id: 1, client: b}]\n", "nodes[0].raft"},
		{"missing client", "groups: [1]\nnodes: [{id: 1, raft: a}]\n", "nodes[0].client"},
		{"dup addr", "groups: [1]\nnodes: [{id: 1, raft: same, client: b}, {id: 2, raft: same, client: d}, {id: 3, raft: e, client: f}]\n", "already used"},
		{"unknown field", "groups: [1]\nnodes: []\nbogus: 1\n", "field bogus"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "c.yaml")
			if err := os.WriteFile(path, []byte(tc.yaml), 0o644); err != nil {
				t.Fatal(err)
			}
			_, err := LoadFileConfig(path)
			if err == nil {
				t.Fatalf("expected error mentioning %q", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q does not mention %q", err, tc.want)
			}
		})
	}
}

func TestValidConfigLoadsAndDerives(t *testing.T) {
	yaml := `groups: [30, 10, 20]
nodes:
  - {id: 1, raft: "127.0.0.1:1", client: "127.0.0.1:2"}
  - {id: 3, raft: "127.0.0.1:3", client: "127.0.0.1:4"}
  - {id: 2, raft: "127.0.0.1:5", client: "127.0.0.1:6"}
`
	path := filepath.Join(t.TempDir(), "c.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0o644); err != nil {
		t.Fatal(err)
	}
	fc, err := LoadFileConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprint(fc.SortedGroups()); got != "[10 20 30]" {
		t.Fatalf("SortedGroups = %s", got)
	}
	if len(fc.Peers()) != 3 || fc.Peers()[2] != "127.0.0.1:5" {
		t.Fatalf("Peers = %v", fc.Peers())
	}
	sm := fc.ShardMap()
	if len(sm.Groups()) != 3 {
		t.Fatalf("shard map uses %d groups", len(sm.Groups()))
	}
}

// TestClusterBootstrapFromConfig boots a full 3-node/3-group cluster the way
// basalt-cluster does, and drives sharded put/get through the client.
func TestClusterBootstrapFromConfig(t *testing.T) {
	ids := []uint64{1, 2, 3}
	raftAddr := map[uint64]string{}
	clientAddr := map[uint64]string{}
	for _, id := range ids {
		rl, ra := freePort(t)
		kl, ka := freePort(t)
		_ = rl.Close()
		_ = kl.Close()
		raftAddr[id], clientAddr[id] = ra, ka
	}
	yaml := "groups: [10, 20, 30]\nnodes:\n"
	for _, id := range ids {
		yaml += fmt.Sprintf("  - {id: %d, raft: %q, client: %q}\n", id, raftAddr[id], clientAddr[id])
	}
	path := filepath.Join(t.TempDir(), "cluster.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0o644); err != nil {
		t.Fatal(err)
	}
	fc, err := LoadFileConfig(path)
	if err != nil {
		t.Fatal(err)
	}

	nodes := map[uint64]*Node{}
	for _, id := range ids {
		self, _ := fc.NodeByID(id)
		n, err := Open(Config{
			ID: id, Peers: fc.Peers(), DataDir: filepath.Join(t.TempDir(), fmt.Sprintf("n%d", id)),
			ElectionTick: 10, HeartbeatTick: 1, TickInterval: 10 * time.Millisecond,
			Groups: fc.SortedGroups(),
		})
		if err != nil {
			t.Fatal(err)
		}
		nodes[id] = n
		rl, err := net.Listen("tcp", self.Raft)
		if err != nil {
			t.Fatal(err)
		}
		kl, err := net.Listen("tcp", self.Client)
		if err != nil {
			t.Fatal(err)
		}
		srv := n.ServeSharded(rl, kl, fc.ShardMap())
		t.Cleanup(func() { srv.Raft.Stop(); srv.KV.Stop() })
	}
	t.Cleanup(func() {
		for _, n := range nodes {
			_ = n.Close()
		}
	})

	// Every group elects a leader.
	for _, gid := range fc.SortedGroups() {
		waitGroupLeader(t, nodes, gid, 20*time.Second)
	}

	// A client hitting any node's client port can put and get keys that
	// route to different shards/groups.
	conn, err := grpc.NewClient(fc.ClientAddrs()[1], grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	c := basaltv1.NewKVServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	served := 0
	for i := 0; i < 60; i++ {
		key := fmt.Sprintf("key-%03d", i)
		_, err := c.Put(ctx, &basaltv1.PutRequest{Key: []byte(key), Value: []byte(fmt.Sprintf("v-%d", i))})
		if err != nil {
			// This node may not lead the owning group; a real client
			// redirects. Here we just tolerate and count successes.
			continue
		}
		resp, err := c.Get(ctx, &basaltv1.GetRequest{Key: []byte(key)})
		if err == nil && resp.GetFound() && string(resp.GetValue()) == fmt.Sprintf("v-%d", i) {
			served++
		}
	}
	if served == 0 {
		t.Fatal("no sharded put/get round-tripped through the bootstrapped cluster")
	}
	t.Logf("bootstrapped 3-node/3-group cluster; %d/60 keys round-tripped via node 1", served)
}
