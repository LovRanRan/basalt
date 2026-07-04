package cluster

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	basaltv1 "github.com/LovRanRan/basalt/api/basalt/v1"
	"github.com/LovRanRan/basalt/internal/shard"
)

func TestShardMapDistribution(t *testing.T) {
	groups := []uint64{10, 20, 30}
	sc := startSharded(t, groups)
	sc.waitAllGroupsLed(t, groups)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Publish a new map version: drain group 30's slots onto group 10.
	base := sc.nodes[1].ShardMap()
	next := base.WithReassign(30, 10) // epoch base+1
	reached, err := sc.nodes[1].DistributeShardMap(ctx, next)
	if err != nil {
		t.Fatalf("distribute: %v", err)
	}
	if reached != 3 {
		t.Fatalf("distribute reached %d nodes, want 3", reached)
	}
	for id, n := range sc.nodes {
		if e := n.ShardMap().Epoch; e != next.Epoch {
			t.Fatalf("node %d at epoch %d, want %d", id, e, next.Epoch)
		}
	}

	// The monotonic fence: an older-epoch map is never adopted, locally or
	// via distribution — no node regresses.
	if sc.nodes[2].SetShardMap(base) {
		t.Fatal("stale map adopted locally")
	}
	if reached, err = sc.nodes[1].DistributeShardMap(ctx, base); err != nil {
		t.Fatalf("distribute stale: %v", err)
	} else if reached != 3 { // every node is already at or past base's epoch
		t.Fatalf("stale distribute reported %d at-target, want 3", reached)
	}
	for id, n := range sc.nodes {
		if e := n.ShardMap().Epoch; e != next.Epoch {
			t.Fatalf("node %d regressed to epoch %d", id, e)
		}
	}

	// GetShardMap over the wire returns the live version.
	resp, err := sc.nodes[1].shardPeers[2].GetShardMap(ctx, &basaltv1.GetShardMapRequest{})
	if err != nil {
		t.Fatalf("get shard map: %v", err)
	}
	rm, err := shard.Unmarshal(resp.GetMap())
	if err != nil || rm.Epoch != next.Epoch {
		t.Fatalf("fetched map epoch = %d (err %v), want %d", rm.Epoch, err, next.Epoch)
	}
}

func TestDynamicGroupBootstrap(t *testing.T) {
	groups := []uint64{10, 20, 30}
	sc := startSharded(t, groups)
	sc.waitAllGroupsLed(t, groups)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Instantiate a brand-new group 40 on every running node — no restart.
	if err := sc.nodes[1].BootstrapGroup(ctx, 40); err != nil {
		t.Fatalf("bootstrap group 40: %v", err)
	}
	waitGroupLeader(t, sc.nodes, 40, 20*time.Second)

	// Publish a new map that moves group 30's slots onto the new group 40.
	base := sc.nodes[1].ShardMap()
	next := base.WithReassign(30, 40)
	if reached, err := sc.nodes[1].DistributeShardMap(ctx, next); err != nil || reached != 3 {
		t.Fatalf("distribute: reached %d, err %v", reached, err)
	}

	// A key that now routes to group 40 is served by the runtime-created
	// group end to end, through the front door.
	var key string
	for i := 0; ; i++ {
		k := fmt.Sprintf("k-%d", i)
		if next.Lookup([]byte(k)) == 40 {
			key = k
			break
		}
		if i > 100000 {
			t.Fatal("no key routes to group 40")
		}
	}
	if _, err := sc.clients[1].Put(ctx, &basaltv1.PutRequest{Key: []byte(key), Value: []byte("v40")}); err != nil {
		t.Fatalf("put to new group: %v", err)
	}
	resp, err := sc.clients[2].Get(ctx, &basaltv1.GetRequest{Key: []byte(key)})
	if err != nil || !resp.GetFound() || string(resp.GetValue()) != "v40" {
		t.Fatalf("get from new group = (%q, %v, %v)", resp.GetValue(), resp.GetFound(), err)
	}
}

func TestRouteFenceRejectsOwnershipDisagreement(t *testing.T) {
	// A forwarded request whose routed group disagrees with this node's own
	// lookup is rejected retryably; agreement (or no header) is served. Note
	// the fence keys on the routed group, not the whole-map epoch, so an
	// unrelated slot moving never fences a key whose owner is unchanged.
	skv := &shardKV{n: &Node{}}

	disagree := metadata.NewIncomingContext(context.Background(), metadata.Pairs(routeGIDMD, "8"))
	if err := skv.fenceRoute(disagree, 7); status.Code(err) != codes.Unavailable {
		t.Fatalf("ownership disagreement: got %v, want Unavailable", err)
	}
	agree := metadata.NewIncomingContext(context.Background(), metadata.Pairs(routeGIDMD, "7"))
	if err := skv.fenceRoute(agree, 7); err != nil {
		t.Fatalf("agreeing forward fenced: %v", err)
	}
	if err := skv.fenceRoute(context.Background(), 7); err != nil {
		t.Fatalf("headerless request fenced: %v", err)
	}
}

func TestConcurrentAddGroupIsIdempotent(t *testing.T) {
	// Many concurrent adds of the same new group must all succeed (no engine
	// flock collision) and the group ends up hosted exactly once.
	groups := []uint64{10}
	sc := startSharded(t, groups)
	sc.waitAllGroupsLed(t, groups)

	n := sc.nodes[1]
	const gid = 77
	errs := make(chan error, 8)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); errs <- n.AddGroup(gid) }()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent AddGroup returned a spurious error: %v", err)
		}
	}
	if n.Group(gid) == nil {
		t.Fatal("group not hosted after concurrent adds")
	}
}

func TestAddGroupAfterCloseRejected(t *testing.T) {
	// A group added to a closed node must not leak: AddGroup refuses and the
	// node reports no such group.
	groups := []uint64{10}
	sc := startSharded(t, groups)
	sc.waitAllGroupsLed(t, groups)

	n := sc.nodes[1]
	if err := n.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := n.AddGroup(99); err == nil {
		t.Fatal("AddGroup on a closed node must fail")
	}
	if n.Group(99) != nil {
		t.Fatal("closed node installed a group")
	}
}
