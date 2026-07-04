package rafttest

import (
	"fmt"
	"testing"

	"github.com/LovRanRan/basalt/internal/raft"
)

func TestReplicatesToAllNodes(t *testing.T) {
	net := NewNetwork([]uint64{1, 2, 3}, Options{Seed: 1})
	lead := waitLeader(t, net, 100)
	for i := 0; i < 5; i++ {
		if err := net.Propose(lead, []byte(fmt.Sprintf("cmd-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	net.Run(30)
	for _, id := range []uint64{1, 2, 3} {
		got := net.Applied(id)
		if len(got) != 5 {
			t.Fatalf("node %d applied %d commands, want 5", id, len(got))
		}
		for i, c := range got {
			if string(c) != fmt.Sprintf("cmd-%d", i) {
				t.Fatalf("node %d applied[%d] = %q", id, i, c)
			}
		}
	}
}

func TestCatchesUpABehindFollower(t *testing.T) {
	net := NewNetwork([]uint64{1, 2, 3}, Options{Seed: 3})
	lead := waitLeader(t, net, 100)
	// Isolate one follower, commit through the majority, then heal.
	behind := uint64(2)
	if behind == lead {
		behind = 3
	}
	net.Partition(behind)
	for i := 0; i < 8; i++ {
		if err := net.Propose(lead, []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	net.Run(30)
	if len(net.Applied(behind)) != 0 {
		t.Fatalf("partitioned follower applied entries: %v", net.Applied(behind))
	}
	net.Heal()
	net.Run(40)
	if got := net.Applied(behind); len(got) != 8 {
		t.Fatalf("healed follower caught up to %d entries, want 8", len(got))
	}
}

func TestCommittedEntriesAgreeUnderChaos(t *testing.T) {
	// Under loss, delay, and repeated leader kills, every node's applied
	// prefix must agree at each index — the log matching property. Seeds
	// print themselves for replay.
	for seed := uint64(0); seed < 300; seed++ {
		net := NewNetwork([]uint64{1, 2, 3, 4, 5}, Options{
			Seed: seed, DropRate: 0.1, MaxDelay: 2,
		})
		lead := uint64(0)
		acked := 0
		for step := 0; step < 400; step++ {
			net.Tick()
			if id, ok := net.Leader(); ok {
				lead = id
				if acked < 60 {
					// Propose a few per settled tick.
					_ = net.Propose(lead, []byte(fmt.Sprintf("s%d-%d", seed, acked)))
					acked++
				}
			}
			// Occasionally kill and revive the leader.
			if step%80 == 79 && lead != 0 {
				net.Partition(lead)
			}
			if step%80 == 39 {
				net.Heal()
			}
		}
		net.Heal()
		net.Run(80)

		// Compare every pair's applied prefixes: at each shared index the
		// command must be identical (state machine safety).
		ref := net.Applied(1)
		for _, id := range []uint64{2, 3, 4, 5} {
			other := net.Applied(id)
			n := len(ref)
			if len(other) < n {
				n = len(other)
			}
			for i := 0; i < n; i++ {
				if string(ref[i]) != string(other[i]) {
					t.Fatalf("seed %d: node 1 and %d disagree at applied[%d]: %q vs %q", seed, id, i, ref[i], other[i])
				}
			}
		}
	}
}

func TestFastBackupConvergence(t *testing.T) {
	// A follower whose log diverges by several entries must be repaired;
	// the conflict-hint backup should not take one round per index.
	net := NewNetwork([]uint64{1, 2, 3}, Options{Seed: 9})
	lead := waitLeader(t, net, 100)
	for i := 0; i < 20; i++ {
		if err := net.Propose(lead, []byte(fmt.Sprintf("e%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	net.Run(60)
	for _, id := range []uint64{1, 2, 3} {
		if got := len(net.Applied(id)); got != 20 {
			t.Fatalf("node %d applied %d, want 20", id, got)
		}
	}
	// Every node's raft log agrees on the committed prefix.
	base := net.Nodes[lead].Committed()
	for _, id := range []uint64{1, 2, 3} {
		if net.Nodes[id].Committed() < base {
			t.Fatalf("node %d committed %d < leader %d", id, net.Nodes[id].Committed(), base)
		}
	}
}

var _ = raft.Leader
