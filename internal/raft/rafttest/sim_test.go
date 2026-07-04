package rafttest

import (
	"testing"

	"github.com/LovRanRan/basalt/internal/raft"
)

// waitLeader runs until exactly one leader exists or the tick budget is
// spent.
func waitLeader(t *testing.T, net *Network, budget int) uint64 {
	t.Helper()
	for i := 0; i < budget; i++ {
		net.Tick()
		if id, ok := net.Leader(); ok {
			return id
		}
	}
	t.Fatal("no single leader within budget")
	return 0
}

func TestElectsUnderPerfectLink(t *testing.T) {
	net := NewNetwork([]uint64{1, 2, 3}, Options{Seed: 1})
	lead := waitLeader(t, net, 100)
	for _, id := range []uint64{1, 2, 3} {
		if net.Nodes[id].Term() != net.Nodes[lead].Term() {
			t.Fatalf("node %d term %d != leader term %d", id, net.Nodes[id].Term(), net.Nodes[lead].Term())
		}
	}
	// Proposals are accepted by the leader (replication assertions are
	// P3.4's; here we only require the leader takes them).
	if err := net.Propose(lead, []byte("a")); err != nil {
		t.Fatalf("leader rejected proposal: %v", err)
	}
	if err := net.Propose(2, []byte("x")); err == nil && net.Nodes[2].Role() != raft.Leader {
		t.Fatal("non-leader accepted a proposal")
	}
}

func TestSeededElectionSafetyUnderChaos(t *testing.T) {
	// Every seed elects at most one leader per term despite loss, delay,
	// and duplication; a failing seed prints itself for exact replay.
	for seed := uint64(0); seed < 2000; seed++ {
		net := NewNetwork([]uint64{1, 2, 3, 4, 5}, Options{
			Seed: seed, DropRate: 0.2, DupRate: 0.1, MaxDelay: 3,
		})
		net.Run(200)
		terms := map[uint64]uint64{}
		for _, id := range []uint64{1, 2, 3, 4, 5} {
			n := net.Nodes[id]
			if n.Role() != raft.Leader {
				continue
			}
			if prev, ok := terms[n.Term()]; ok {
				t.Fatalf("seed %d: nodes %d and %d both lead term %d", seed, prev, id, n.Term())
			}
			terms[n.Term()] = id
		}
	}
}

func TestReplaySameSeedIsIdentical(t *testing.T) {
	run := func() (uint64, uint64) {
		net := NewNetwork([]uint64{1, 2, 3}, Options{Seed: 42, DropRate: 0.1, MaxDelay: 2})
		var lead, tick uint64
		for i := 0; i < 100; i++ {
			net.Tick()
			tick++
			if id, ok := net.Leader(); ok {
				lead = id
				break
			}
		}
		net.Run(30)
		return lead, tick
	}
	l1, t1 := run()
	l2, t2 := run()
	if l1 != l2 || t1 != t2 {
		t.Fatalf("replay diverged: leader %d/%d, elected-at %d/%d", l1, l2, t1, t2)
	}
}

func TestPartitionedLeaderIsSupplanted(t *testing.T) {
	net := NewNetwork([]uint64{1, 2, 3}, Options{Seed: 7})
	lead := waitLeader(t, net, 100)
	oldTerm := net.Nodes[lead].Term()

	// Isolate the leader; the majority side must elect a new one at a
	// higher term.
	net.Partition(lead)
	var newLead uint64
	for i := 0; i < 200; i++ {
		net.Tick()
		if id, ok := net.Leader(); ok && net.Nodes[id].Term() > oldTerm {
			newLead = id
			break
		}
	}
	if newLead == 0 {
		t.Fatal("majority never elected a higher-term leader")
	}
	if newLead == lead {
		t.Fatal("partitioned leader supplanted itself")
	}

	// Heal: the old leader rejoins and steps down to the higher term.
	net.Heal()
	net.Run(30)
	if net.Nodes[lead].Role() == raft.Leader {
		t.Fatal("old leader did not step down after heal")
	}
	if id, ok := net.Leader(); !ok || id != newLead {
		t.Fatalf("cluster did not settle on the new leader: %d ok=%v", id, ok)
	}
}
