package shard

import (
	"fmt"
	"math/rand"
	"testing"
)

func TestSlotDeterministicAndInRange(t *testing.T) {
	for i := 0; i < 10000; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		s1, s2 := Slot(key), Slot(key)
		if s1 != s2 {
			t.Fatalf("Slot not deterministic for %q", key)
		}
		if s1 >= NumSlots {
			t.Fatalf("slot %d out of range", s1)
		}
	}
}

func TestNewShardMapBalanced(t *testing.T) {
	groups := []uint64{10, 20, 30, 40}
	m := NewShardMap(groups)
	counts := map[uint64]int{}
	for s := uint32(0); s < NumSlots; s++ {
		counts[m.Group(s)]++
	}
	if len(counts) != len(groups) {
		t.Fatalf("used %d groups, want %d", len(counts), len(groups))
	}
	// Even spread within one slot of NumSlots/len(groups).
	base := NumSlots / len(groups)
	for g, c := range counts {
		if c < base-1 || c > base+1 {
			t.Fatalf("group %d owns %d slots, want ~%d", g, c, base)
		}
	}
	if m.Epoch != 1 {
		t.Fatalf("fresh map epoch = %d, want 1", m.Epoch)
	}
	if g := NewShardMap(nil).Lookup([]byte("x")); g != 0 {
		t.Fatalf("empty map lookup = %d, want 0 (unassigned)", g)
	}
}

func TestLookupMatchesSlotOwner(t *testing.T) {
	m := NewShardMap([]uint64{1, 2, 3})
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 5000; i++ {
		key := []byte(fmt.Sprintf("k%d", rng.Intn(1<<20)))
		if m.Lookup(key) != m.Group(Slot(key)) {
			t.Fatalf("Lookup and Group disagree for %q", key)
		}
	}
}

func TestWithSlotMovesOnlyThatSlot(t *testing.T) {
	m := NewShardMap([]uint64{1, 2, 3})
	slot := uint32(42)
	orig := m.Group(slot)
	target := orig + 1
	nm := m.WithSlot(slot, target)

	if nm.Epoch != m.Epoch+1 {
		t.Fatalf("epoch = %d, want %d", nm.Epoch, m.Epoch+1)
	}
	if nm.Group(slot) != target {
		t.Fatalf("slot %d not reassigned", slot)
	}
	if m.Group(slot) != orig {
		t.Fatal("WithSlot mutated the receiver")
	}
	// Every other slot is untouched — the point of fixed slots.
	for s := uint32(0); s < NumSlots; s++ {
		if s == slot {
			continue
		}
		if nm.Group(s) != m.Group(s) {
			t.Fatalf("slot %d changed by a single-slot move", s)
		}
	}
}

func TestWithReassignDrainsAGroup(t *testing.T) {
	m := NewShardMap([]uint64{1, 2, 3})
	before := len(m.SlotsFor(1))
	if before == 0 {
		t.Fatal("group 1 owns no slots to move")
	}
	// Add group 4 by moving all of group 1's slots to it.
	nm := m.WithReassign(1, 4)
	if len(nm.SlotsFor(1)) != 0 {
		t.Fatal("group 1 still owns slots after drain")
	}
	if len(nm.SlotsFor(4)) != before {
		t.Fatalf("group 4 owns %d slots, want %d", len(nm.SlotsFor(4)), before)
	}
	// Groups 2 and 3 are untouched.
	if len(nm.SlotsFor(2)) != len(m.SlotsFor(2)) || len(nm.SlotsFor(3)) != len(m.SlotsFor(3)) {
		t.Fatal("reassign disturbed unrelated groups")
	}
	if nm.Epoch != m.Epoch+1 {
		t.Fatalf("epoch = %d, want %d", nm.Epoch, m.Epoch+1)
	}
}

func TestGroupsAndSlotsForConsistent(t *testing.T) {
	m := NewShardMap([]uint64{5, 6, 7})
	total := 0
	for _, g := range m.Groups() {
		total += len(m.SlotsFor(g))
	}
	if total != NumSlots {
		t.Fatalf("slots across groups = %d, want %d", total, NumSlots)
	}
}

func FuzzSlotStableAndInRange(f *testing.F) {
	f.Add([]byte(""))
	f.Add([]byte("key-1"))
	f.Add([]byte{0x00, 0xff, 0x00})
	m := NewShardMap([]uint64{1, 2, 3, 4, 5})
	f.Fuzz(func(t *testing.T, key []byte) {
		s := Slot(key)
		if s >= NumSlots {
			t.Fatalf("slot %d out of range for %q", s, key)
		}
		if Slot(key) != s {
			t.Fatal("Slot not stable")
		}
		if m.Lookup(key) != m.Group(s) {
			t.Fatal("Lookup disagrees with Group(Slot)")
		}
	})
}

func TestMarshalRoundtrip(t *testing.T) {
	m := NewShardMap([]uint64{7, 8, 9}).WithSlot(3, 99).WithMigrating(5, true).WithMigrating(200, true)
	got, err := Unmarshal(m.Marshal())
	if err != nil {
		t.Fatal(err)
	}
	if got.Epoch != m.Epoch {
		t.Fatalf("epoch = %d, want %d", got.Epoch, m.Epoch)
	}
	for s := uint32(0); s < NumSlots; s++ {
		if got.Group(s) != m.Group(s) {
			t.Fatalf("slot %d = %d, want %d", s, got.Group(s), m.Group(s))
		}
		if got.IsMigrating(s) != m.IsMigrating(s) {
			t.Fatalf("slot %d migrating = %v, want %v", s, got.IsMigrating(s), m.IsMigrating(s))
		}
	}
	if !got.IsMigrating(5) || !got.IsMigrating(200) || got.IsMigrating(6) {
		t.Fatal("migrating bitset did not round-trip")
	}
	if _, err := Unmarshal([]byte{1, 2}); err == nil {
		t.Fatal("short data must fail")
	}
}

func TestWithSlotClearsMigrating(t *testing.T) {
	m := NewShardMap([]uint64{1, 2}).WithMigrating(4, true)
	if !m.IsMigrating(4) {
		t.Fatal("slot 4 should be migrating")
	}
	flipped := m.WithSlot(4, 2)
	if flipped.IsMigrating(4) {
		t.Fatal("reassigning a slot must clear its migrating flag")
	}
	if flipped.Epoch != m.Epoch+1 {
		t.Fatalf("epoch = %d, want %d", flipped.Epoch, m.Epoch+1)
	}
}
