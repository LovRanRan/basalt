package cluster

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"testing"
	"time"

	basalt "github.com/LovRanRan/basalt"
	"github.com/LovRanRan/basalt/internal/shard"
)

// colocateLeader makes target the leader of group gid and waits until it can
// serve a ReadIndex (i.e. its no-op has committed), so a subsequent CopySlot
// barrier on that group succeeds without a just-elected-leader retry.
func colocateLeader(t *testing.T, ctx context.Context, sc *shardedCluster, gid, target uint64) {
	t.Helper()
	cur := waitGroupLeader(t, sc.nodes, gid, 20*time.Second)
	if cur != target {
		if err := sc.nodes[cur].Group(gid).TransferLeader(ctx, target); err != nil {
			t.Fatalf("transfer group %d to node %d: %v", gid, target, err)
		}
	}
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		g := sc.nodes[target].Group(gid)
		if _, _, isLeader := g.Status(); isLeader {
			if err := g.ReadIndex(ctx); err == nil {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("node %d never became a ready leader of group %d", target, gid)
}

func TestCopySlotMovesOnlyItsKeys(t *testing.T) {
	groups := []uint64{10, 20}
	sc := startSharded(t, groups)
	sc.waitAllGroupsLed(t, groups)
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	smap := sc.smap

	// A slot owned by group 10 (the migration source).
	var slot uint32
	ok := false
	for s := uint32(0); s < shard.NumSlots; s++ {
		if smap.Group(s) == 10 {
			slot, ok = s, true
			break
		}
	}
	if !ok {
		t.Fatal("no slot owned by group 10")
	}

	// Keys that hash into the target slot, plus a control key in a DIFFERENT
	// slot that also belongs to group 10 (must NOT be copied).
	var slotKeys []string
	for i := 0; len(slotKeys) < 20; i++ {
		k := fmt.Sprintf("mig-%d", i)
		if shard.Slot([]byte(k)) == slot {
			slotKeys = append(slotKeys, k)
		}
	}
	var control string
	for i := 0; ; i++ {
		k := fmt.Sprintf("ctl-%d", i)
		s := shard.Slot([]byte(k))
		if s != slot && smap.Group(s) == 10 {
			control = k
			break
		}
	}

	for i, k := range slotKeys {
		sc.mustPut(t, ctx, 1, k, fmt.Sprintf("v%d", i))
	}
	sc.mustPut(t, ctx, 1, control, "control")

	// Co-locate both leaderships on one node, then copy: it leads src (for the
	// read barrier) and dst (to propose into it).
	coord := waitGroupLeader(t, sc.nodes, 20, 20*time.Second)
	colocateLeader(t, ctx, sc, 10, coord)
	dstLeader := coord
	last, copied, err := sc.nodes[dstLeader].CopySlot(ctx, slot, 10, 20, nil)
	if err != nil {
		t.Fatalf("copy slot: %v", err)
	}
	if copied != len(slotKeys) {
		t.Fatalf("copied %d keys, want %d", copied, len(slotKeys))
	}
	if string(last) == "" {
		t.Fatal("copy returned no cursor")
	}

	// dst now holds every slot key with the right value...
	dg := sc.nodes[dstLeader].Group(20).DB()
	for i, k := range slotKeys {
		v, err := dg.Get([]byte(k))
		if err != nil || string(v) != fmt.Sprintf("v%d", i) {
			t.Fatalf("dst missing %q after copy: %q %v", k, v, err)
		}
	}
	// ...but not the control key from another slot.
	if _, err := dg.Get([]byte(control)); !errors.Is(err, basalt.ErrNotFound) {
		t.Fatalf("dst wrongly holds unrelated slot's key %q: %v", control, err)
	}

	// src still holds everything: ownership was not flipped.
	srcLeader := waitGroupLeader(t, sc.nodes, 10, 20*time.Second)
	sgdb := sc.nodes[srcLeader].Group(10).DB()
	for _, k := range slotKeys {
		if _, err := sgdb.Get([]byte(k)); err != nil {
			t.Fatalf("src lost %q after copy: %v", k, err)
		}
	}
	if _, err := sgdb.Get([]byte(control)); err != nil {
		t.Fatalf("src lost control key %q: %v", control, err)
	}

	// Idempotent: a second full copy reproduces the same state, same count.
	if _, copied2, err := sc.nodes[dstLeader].CopySlot(ctx, slot, 10, 20, nil); err != nil || copied2 != len(slotKeys) {
		t.Fatalf("second copy: copied %d err %v", copied2, err)
	}
}

func TestCopySlotResumesFromCursor(t *testing.T) {
	groups := []uint64{10, 20}
	sc := startSharded(t, groups)
	sc.waitAllGroupsLed(t, groups)
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	smap := sc.smap
	var slot uint32
	ok := false
	for s := uint32(0); s < shard.NumSlots; s++ {
		if smap.Group(s) == 10 {
			slot, ok = s, true
			break
		}
	}
	if !ok {
		t.Fatal("no slot owned by group 10")
	}

	var slotKeys []string
	for i := 0; len(slotKeys) < 16; i++ {
		k := fmt.Sprintf("res-%d", i)
		if shard.Slot([]byte(k)) == slot {
			slotKeys = append(slotKeys, k)
		}
	}
	for i, k := range slotKeys {
		sc.mustPut(t, ctx, 1, k, fmt.Sprintf("v%d", i))
	}
	sort.Strings(slotKeys)

	coord := waitGroupLeader(t, sc.nodes, 20, 20*time.Second)
	colocateLeader(t, ctx, sc, 10, coord)
	dstLeader := coord

	// A cursor at the median copies exactly the keys strictly greater than it.
	mid := slotKeys[len(slotKeys)/2]
	wantAfter := 0
	for _, k := range slotKeys {
		if k > mid {
			wantAfter++
		}
	}
	_, copied, err := sc.nodes[dstLeader].CopySlot(ctx, slot, 10, 20, []byte(mid))
	if err != nil || copied != wantAfter {
		t.Fatalf("resume after %q: copied %d, want %d (err %v)", mid, copied, wantAfter, err)
	}

	// A cursor past every key copies nothing.
	if _, copied, err := sc.nodes[dstLeader].CopySlot(ctx, slot, 10, 20, []byte("\xff")); err != nil || copied != 0 {
		t.Fatalf("resume past end: copied %d, want 0 (err %v)", copied, err)
	}
}
