// Package shard maps keys to raft groups through a fixed table of hash
// slots. A key hashes to one of NumSlots slots; a versioned ShardMap
// assigns each slot to a group. Fixed slots (rather than a consistent-hash
// ring) make ownership explicit and migration a clean per-slot operation:
// moving a slot moves exactly the keys that hash to it, and remapping never
// disturbs any other slot.
package shard

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"sort"
)

// NumSlots is the fixed slot count (Redis Cluster uses 16384; 256 keeps the
// map compact while still spreading keys finely across a handful of groups).
const NumSlots = 256

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Slot returns the slot a key belongs to: deterministic and
// platform-independent (crc32c mod NumSlots).
func Slot(key []byte) uint32 {
	return crc32.Checksum(key, castagnoli) % NumSlots
}

// ShardMap is an immutable, versioned assignment of every slot to a group.
// Epoch increases on each change so stale-routed requests can be fenced
// (P4.6). A slot with group 0 is unassigned. A slot marked migrating is being
// handed to a new owner: it still routes reads to its current group, but the
// front door rejects writes to it (retryably) so no write lands after the
// migration's final copy — the write freeze that makes a slot handoff
// single-owner-safe (P4.8).
type ShardMap struct {
	Epoch     uint64
	slots     [NumSlots]uint64 // slot -> group id
	migrating [NumSlots]bool   // slot -> writes frozen for a handoff
}

// NewShardMap spreads the slots as evenly as possible across the given
// groups (sorted for determinism), at epoch 1. Empty groups yield an empty
// map (all slots unassigned).
func NewShardMap(groups []uint64) *ShardMap {
	m := &ShardMap{Epoch: 1}
	if len(groups) == 0 {
		return m
	}
	g := append([]uint64(nil), groups...)
	sort.Slice(g, func(i, j int) bool { return g[i] < g[j] })
	for s := 0; s < NumSlots; s++ {
		m.slots[s] = g[s%len(g)]
	}
	return m
}

// Group returns the group owning slot, or 0 if unassigned.
func (m *ShardMap) Group(slot uint32) uint64 {
	if slot >= NumSlots {
		panic(fmt.Sprintf("shard: slot %d out of range", slot))
	}
	return m.slots[slot]
}

// Lookup returns the group owning a key, or 0 if its slot is unassigned.
func (m *ShardMap) Lookup(key []byte) uint64 {
	return m.slots[Slot(key)]
}

// Groups returns the distinct assigned group ids, sorted.
func (m *ShardMap) Groups() []uint64 {
	seen := map[uint64]bool{}
	for _, g := range m.slots {
		if g != 0 {
			seen[g] = true
		}
	}
	out := make([]uint64, 0, len(seen))
	for g := range seen {
		out = append(out, g)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// SlotsFor returns the slots owned by a group, ascending.
func (m *ShardMap) SlotsFor(group uint64) []uint32 {
	var out []uint32
	for s := uint32(0); s < NumSlots; s++ {
		if m.slots[s] == group {
			out = append(out, s)
		}
	}
	return out
}

// IsMigrating reports whether slot is frozen for a handoff.
func (m *ShardMap) IsMigrating(slot uint32) bool {
	if slot >= NumSlots {
		panic(fmt.Sprintf("shard: slot %d out of range", slot))
	}
	return m.migrating[slot]
}

// IsKeyMigrating reports whether a key's slot is frozen for a handoff.
func (m *ShardMap) IsKeyMigrating(key []byte) bool {
	return m.migrating[Slot(key)]
}

// WithMigrating returns a copy that marks slot frozen (or unfrozen) for a
// handoff, bumping the epoch. Ownership is unchanged, so reads still route to
// the current group; the front door rejects writes while frozen.
func (m *ShardMap) WithMigrating(slot uint32, migrating bool) *ShardMap {
	if slot >= NumSlots {
		panic(fmt.Sprintf("shard: slot %d out of range", slot))
	}
	nm := *m
	nm.migrating[slot] = migrating
	nm.Epoch = m.Epoch + 1
	return &nm
}

// WithSlot returns a copy with slot reassigned to group and the epoch
// bumped — the atomic unit of a rebalance (P4.7/P4.8). Reassigning a slot also
// clears its migrating flag: the flip to the new owner ends the handoff. The
// receiver is unchanged.
func (m *ShardMap) WithSlot(slot uint32, group uint64) *ShardMap {
	if slot >= NumSlots {
		panic(fmt.Sprintf("shard: slot %d out of range", slot))
	}
	nm := *m
	nm.slots[slot] = group
	nm.migrating[slot] = false
	nm.Epoch = m.Epoch + 1
	return &nm
}

// WithReassign returns a copy moving every slot currently owned by from to
// group to, bumping the epoch once — used to add or drain a group wholesale.
func (m *ShardMap) WithReassign(from, to uint64) *ShardMap {
	nm := *m
	for s := 0; s < NumSlots; s++ {
		if nm.slots[s] == from {
			nm.slots[s] = to
		}
	}
	nm.Epoch = m.Epoch + 1
	return &nm
}

// migratingBytes is the packed migrating bitset size (one bit per slot).
const migratingBytes = NumSlots / 8

// Marshal serializes the map for distribution: epoch, one uvarint group id per
// slot, then a packed migrating bitset (one bit per slot).
func (m *ShardMap) Marshal() []byte {
	buf := binary.LittleEndian.AppendUint64(nil, m.Epoch)
	for _, g := range m.slots {
		buf = binary.AppendUvarint(buf, g)
	}
	var bits [migratingBytes]byte
	for s := 0; s < NumSlots; s++ {
		if m.migrating[s] {
			bits[s/8] |= 1 << (uint(s) % 8)
		}
	}
	return append(buf, bits[:]...)
}

// Unmarshal parses a marshaled map.
func Unmarshal(data []byte) (*ShardMap, error) {
	if len(data) < 8 {
		return nil, fmt.Errorf("shard: short map (%d bytes)", len(data))
	}
	m := &ShardMap{Epoch: binary.LittleEndian.Uint64(data)}
	rest := data[8:]
	for s := 0; s < NumSlots; s++ {
		g, n := binary.Uvarint(rest)
		if n <= 0 {
			return nil, fmt.Errorf("shard: truncated slot %d", s)
		}
		m.slots[s] = g
		rest = rest[n:]
	}
	if len(rest) != migratingBytes {
		return nil, fmt.Errorf("shard: expected %d migrating-bitset bytes, got %d", migratingBytes, len(rest))
	}
	for s := 0; s < NumSlots; s++ {
		if rest[s/8]&(1<<(uint(s)%8)) != 0 {
			m.migrating[s] = true
		}
	}
	return m, nil
}
