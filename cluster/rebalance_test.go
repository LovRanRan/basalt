package cluster

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	basalt "github.com/LovRanRan/basalt"
	basaltv1 "github.com/LovRanRan/basalt/api/basalt/v1"
	"github.com/LovRanRan/basalt/internal/shard"
)

// firstSlotOf returns a slot owned by group g in map m.
func firstSlotOf(t *testing.T, m *shard.ShardMap, g uint64) uint32 {
	t.Helper()
	for s := uint32(0); s < shard.NumSlots; s++ {
		if m.Group(s) == g {
			return s
		}
	}
	t.Fatalf("no slot owned by group %d", g)
	return 0
}

// keysInSlot returns n distinct keys that hash into slot.
func keysInSlot(slot uint32, n int) []string {
	var out []string
	for i := 0; len(out) < n; i++ {
		k := fmt.Sprintf("mk-%d", i)
		if shard.Slot([]byte(k)) == slot {
			out = append(out, k)
		}
	}
	return out
}

func TestMigrateSlotEndToEnd(t *testing.T) {
	groups := []uint64{10, 20}
	sc := startSharded(t, groups)
	sc.waitAllGroupsLed(t, groups)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	slot := firstSlotOf(t, sc.smap, 10)
	slotKeys := keysInSlot(slot, 24)
	for i, k := range slotKeys {
		sc.mustPut(t, ctx, 1, k, fmt.Sprintf("v%d", i))
	}
	// A control key in group 20 that must be untouched by the migration.
	control := keysInSlot(firstSlotOf(t, sc.smap, 20), 1)[0]
	sc.mustPut(t, ctx, 1, control, "keep")

	// Co-locate both leaderships, then migrate the slot 10 -> 20.
	coord := waitGroupLeader(t, sc.nodes, 20, 20*time.Second)
	colocateLeader(t, ctx, sc, 10, coord)
	if err := sc.nodes[coord].MigrateSlot(ctx, slot, 10, 20); err != nil {
		t.Fatalf("migrate slot: %v", err)
	}

	// Every node now routes the slot to dst and it is no longer migrating.
	for id, n := range sc.nodes {
		m := n.ShardMap()
		if m.Group(slot) != 20 {
			t.Fatalf("node %d routes slot %d to group %d, want 20", id, slot, m.Group(slot))
		}
		if m.IsMigrating(slot) {
			t.Fatalf("node %d still marks slot %d migrating", id, slot)
		}
	}

	// dst holds every migrated key; src no longer holds any of them.
	dg := sc.nodes[coord].Group(20).DB()
	sgdb := sc.nodes[coord].Group(10).DB()
	for i, k := range slotKeys {
		if v, err := dg.Get([]byte(k)); err != nil || string(v) != fmt.Sprintf("v%d", i) {
			t.Fatalf("dst missing migrated key %q: %q %v", k, v, err)
		}
		if _, err := sgdb.Get([]byte(k)); !errors.Is(err, basalt.ErrNotFound) {
			t.Fatalf("src still holds migrated key %q after cleanup: %v", k, err)
		}
	}
	// The control key in group 20 is intact.
	if v, err := dg.Get([]byte(control)); err != nil || string(v) != "keep" {
		t.Fatalf("control key disturbed: %q %v", v, err)
	}

	// The front door now serves the slot from its new owner: reads see the
	// migrated data and fresh writes land and read back.
	resp := sc.mustGet(t, ctx, 2, slotKeys[0])
	if !resp.GetFound() || string(resp.GetValue()) != "v0" {
		t.Fatalf("post-migration read = (%q, %v)", resp.GetValue(), resp.GetFound())
	}
	newKey := keysInSlot(slot, 25)[24] // another key in the migrated slot
	sc.mustPut(t, ctx, 3, newKey, "after")
	if r := sc.mustGet(t, ctx, 1, newKey); !r.GetFound() || string(r.GetValue()) != "after" {
		t.Fatalf("post-migration write not served: (%q, %v)", r.GetValue(), r.GetFound())
	}

	// Idempotent: re-running the completed migration is a no-op (resumes at
	// cleanup, finds nothing to delete).
	if err := sc.nodes[coord].MigrateSlot(ctx, slot, 10, 20); err != nil {
		t.Fatalf("re-running completed migration: %v", err)
	}
}

func TestScanDuringMigrationHasNoDuplicates(t *testing.T) {
	// In the mid-migration state — a slot's keys copied into dst but ownership
	// not yet flipped — a full scan must still return each key exactly once
	// (from the current owner), not once per group holding a copy.
	groups := []uint64{10, 20}
	sc := startSharded(t, groups)
	sc.waitAllGroupsLed(t, groups)
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	slot := firstSlotOf(t, sc.smap, 10)
	slotKeys := keysInSlot(slot, 20)
	want := map[string]string{}
	for i, k := range slotKeys {
		v := fmt.Sprintf("v%d", i)
		sc.mustPut(t, ctx, 1, k, v)
		want[k] = v
	}
	// Also write a few keys owned by group 20 so the scan spans both groups.
	for i, k := range keysInSlot(firstSlotOf(t, sc.smap, 20), 5) {
		v := fmt.Sprintf("w%d", i)
		sc.mustPut(t, ctx, 1, k, v)
		want[k] = v
	}

	// Reach the partial state: freeze + copy into dst, but do NOT flip.
	coord := waitGroupLeader(t, sc.nodes, 20, 20*time.Second)
	colocateLeader(t, ctx, sc, 10, coord)
	if err := sc.nodes[coord].publishToAll(ctx, sc.nodes[coord].ShardMap().WithMigrating(slot, true)); err != nil {
		t.Fatalf("freeze: %v", err)
	}
	if _, _, err := sc.nodes[coord].CopySlot(ctx, slot, 10, 20, nil); err != nil {
		t.Fatalf("copy: %v", err)
	}
	// Now group 20's engine holds the slot's keys too, but ownership is still
	// group 10. A full scan must not double-count them.
	got := map[string]int{}
	var vals []*basaltv1.KeyValue
	retryRPC(t, ctx, func() error {
		stream, err := sc.clients[1].Scan(ctx, &basaltv1.ScanRequest{})
		if err != nil {
			return err
		}
		acc := map[string]int{}
		var accVals []*basaltv1.KeyValue
		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}
			for _, kv := range resp.GetPairs() {
				acc[string(kv.GetKey())]++
				accVals = append(accVals, kv)
			}
		}
		got, vals = acc, accVals
		return nil
	})
	for k, n := range got {
		if n != 1 {
			t.Fatalf("key %q returned %d times during migration, want 1", k, n)
		}
	}
	if len(vals) != len(want) {
		t.Fatalf("scan returned %d pairs, want %d", len(vals), len(want))
	}
	for _, kv := range vals {
		if want[string(kv.GetKey())] != string(kv.GetValue()) {
			t.Fatalf("scan value mismatch for %q: %q", kv.GetKey(), kv.GetValue())
		}
	}
}

func TestFrozenSlotRejectsWritesAllowsReads(t *testing.T) {
	groups := []uint64{10, 20}
	sc := startSharded(t, groups)
	sc.waitAllGroupsLed(t, groups)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	slot := firstSlotOf(t, sc.smap, 10)
	key := keysInSlot(slot, 1)[0]
	sc.mustPut(t, ctx, 1, key, "v0")

	// Freeze the slot cluster-wide.
	if err := sc.nodes[1].publishToAll(ctx, sc.nodes[1].ShardMap().WithMigrating(slot, true)); err != nil {
		t.Fatalf("publish freeze: %v", err)
	}

	// A write to the frozen slot is rejected with the retryable
	// slot-migrating signal; a read still succeeds from the current owner.
	// Use raw stubs so the test observes the rejection itself.
	wantFrozen := func(err error) bool {
		st, ok := status.FromError(err)
		return ok && st.Code() == codes.FailedPrecondition && strings.HasPrefix(st.Message(), migratingPrefix)
	}
	if _, err := sc.clients[1].Put(ctx, &basaltv1.PutRequest{Key: []byte(key), Value: []byte("v1")}); !wantFrozen(err) {
		t.Fatalf("write to frozen slot = %v, want slot-migrating FailedPrecondition", err)
	}
	if _, err := sc.clients[2].Delete(ctx, &basaltv1.DeleteRequest{Key: []byte(key)}); !wantFrozen(err) {
		t.Fatalf("delete on frozen slot = %v, want slot-migrating FailedPrecondition", err)
	}
	resp, err := sc.clients[1].Get(ctx, &basaltv1.GetRequest{Key: []byte(key)})
	if err != nil || !resp.GetFound() || string(resp.GetValue()) != "v0" {
		t.Fatalf("read of frozen slot = (%q, %v, %v), want v0", resp.GetValue(), resp.GetFound(), err)
	}

	// Unfreeze restores writes.
	if err := sc.nodes[1].publishToAll(ctx, sc.nodes[1].ShardMap().WithMigrating(slot, false)); err != nil {
		t.Fatalf("publish unfreeze: %v", err)
	}
	sc.mustPut(t, ctx, 1, key, "v2")
	if r := sc.mustGet(t, ctx, 1, key); !r.GetFound() || string(r.GetValue()) != "v2" {
		t.Fatalf("post-unfreeze write = (%q, %v)", r.GetValue(), r.GetFound())
	}
}

func TestMigrateAbortRollsBackFreeze(t *testing.T) {
	// A migration aborted after the freeze is published (here: one node
	// unreachable fails publishToAll's all-nodes requirement) must roll the
	// freeze back on the live nodes, or the slot's writes stay rejected for
	// the whole outage.
	groups := []uint64{10, 20}
	sc := startSharded(t, groups)
	sc.waitAllGroupsLed(t, groups)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	slot := firstSlotOf(t, sc.smap, 10)
	key := keysInSlot(slot, 1)[0]
	sc.mustPut(t, ctx, 1, key, "v0")

	coord := waitGroupLeader(t, sc.nodes, 20, 20*time.Second)
	colocateLeader(t, ctx, sc, 10, coord)

	// Take down a third node's peer server (it hosts ShardService), so the
	// freeze publish cannot reach every member.
	var down uint64
	for _, id := range []uint64{1, 2, 3} {
		if id != coord {
			down = id
			break
		}
	}
	sc.srvs[down].Raft.Stop()

	if err := sc.nodes[coord].MigrateSlot(ctx, slot, 10, 20); err == nil {
		t.Fatal("migration with a node down must fail the all-nodes publish")
	}
	// The rollback ran: no live node still has the slot frozen, and writes to
	// it work again.
	for _, id := range []uint64{1, 2, 3} {
		if id == down {
			continue
		}
		if sc.nodes[id].ShardMap().IsMigrating(slot) {
			t.Fatalf("node %d left frozen after aborted migration", id)
		}
	}
	sc.mustPut(t, ctx, coord, key, "v1")
	if r := sc.mustGet(t, ctx, coord, key); !r.GetFound() || string(r.GetValue()) != "v1" {
		t.Fatalf("write after aborted migration = (%q, %v)", r.GetValue(), r.GetFound())
	}
}

func TestClientBacksOffThroughFreeze(t *testing.T) {
	// cluster.Client must treat the slot-migrating rejection as back-off-and-
	// retry-in-place, succeeding once the handoff ends — not burn its hop
	// budget rotating nodes and surface a bogus "no leader" error.
	groups := []uint64{10, 20}
	sc := startSharded(t, groups)
	sc.waitAllGroupsLed(t, groups)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	slot := firstSlotOf(t, sc.smap, 10)
	key := keysInSlot(slot, 1)[0]

	if err := sc.nodes[1].publishToAll(ctx, sc.nodes[1].ShardMap().WithMigrating(slot, true)); err != nil {
		t.Fatalf("freeze: %v", err)
	}
	go func() {
		time.Sleep(300 * time.Millisecond)
		uctx, ucancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer ucancel()
		_ = sc.nodes[1].publishToAll(uctx, sc.nodes[1].ShardMap().WithMigrating(slot, false))
	}()

	cl, err := NewClient(sc.kvAddrs)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if err := cl.Put(ctx, []byte(key), []byte("thawed")); err != nil {
		t.Fatalf("client put through freeze: %v", err)
	}
	v, found, err := cl.Get(ctx, []byte(key))
	if err != nil || !found || string(v) != "thawed" {
		t.Fatalf("get after thaw = (%q, %v, %v)", v, found, err)
	}
}
