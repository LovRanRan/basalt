package raft

import "testing"

func TestConfChangeCodec(t *testing.T) {
	for _, cc := range []ConfChange{
		{ConfAddNode, 5},
		{ConfRemoveNode, 42},
		{ConfAddLearner, 1 << 40},
	} {
		got, err := decodeConfChange(cc.Encode())
		if err != nil || got != cc {
			t.Fatalf("roundtrip %+v = %+v, %v", cc, got, err)
		}
	}
	if _, err := decodeConfChange([]byte{1, 2, 3}); err == nil {
		t.Fatal("short conf change must fail")
	}
}

func TestLeadershipTransfer(t *testing.T) {
	nodes := cluster(1, 2, 3)
	nodes[1].Campaign()
	deliver(t, nodes, nil)
	if nodes[1].Role() != Leader {
		t.Fatal("node 1 should lead")
	}
	// Replicate an entry so the target is caught up.
	if err := nodes[1].Propose([]byte("x")); err != nil {
		t.Fatal(err)
	}
	deliver(t, nodes, nil)

	if !nodes[1].TransferLeader(2) {
		t.Fatal("transfer to node 2 should start")
	}
	// While transferring, the old leader stops accepting proposals.
	if err := nodes[1].Propose([]byte("blocked")); err != ErrNotLeader {
		t.Fatalf("proposal during transfer = %v, want ErrNotLeader", err)
	}
	deliver(t, nodes, nil)
	if nodes[2].Role() != Leader {
		t.Fatalf("node 2 did not take leadership: role=%v", nodes[2].Role())
	}
	if nodes[1].Role() == Leader {
		t.Fatal("old leader still leads after transfer")
	}
	if nodes[2].Term() <= 1 {
		t.Fatalf("new leader term %d not advanced", nodes[2].Term())
	}
}

func TestTransferToNonVoterOrSelfRejected(t *testing.T) {
	nodes := cluster(1, 2, 3)
	nodes[1].Campaign()
	deliver(t, nodes, nil)
	if nodes[1].TransferLeader(1) {
		t.Fatal("transfer to self must be rejected")
	}
	if nodes[1].TransferLeader(99) {
		t.Fatal("transfer to unknown node must be rejected")
	}
	if nodes[2].TransferLeader(1) {
		t.Fatal("a follower cannot transfer leadership")
	}
}

func TestAddLearnerThenPromote(t *testing.T) {
	// Start a 3-node cluster; add node 4 as a learner, let it catch up,
	// then promote it to a voter. The quorum size only changes on promote.
	nodes := cluster(1, 2, 3)
	nodes[4] = NewNode(4, []uint64{1, 2, 3, 4}) // node 4 knows the target config
	nodes[1].Campaign()
	deliver(t, nodes, func(m Message) bool { return m.To == 4 || m.From == 4 })
	if nodes[1].Role() != Leader {
		t.Fatal("node 1 should lead")
	}
	before := nodes[1].NumVoters()

	// Add node 4 as a learner: it receives the log but quorum is unchanged.
	if err := nodes[1].ProposeConfChange(ConfChange{ConfAddLearner, 4}); err != nil {
		t.Fatal(err)
	}
	deliver(t, nodes, nil)
	if !nodes[1].IsLearner(4) {
		t.Fatal("node 4 not a learner")
	}
	if nodes[1].NumVoters() != before {
		t.Fatalf("learner changed voter count: %d -> %d", before, nodes[1].NumVoters())
	}
	// Replicate an entry; node 4 (learner) receives it.
	if err := nodes[1].Propose([]byte("v1")); err != nil {
		t.Fatal(err)
	}
	deliver(t, nodes, nil)

	// Promote node 4 to a voter.
	if err := nodes[1].ProposeConfChange(ConfChange{ConfAddNode, 4}); err != nil {
		t.Fatal(err)
	}
	deliver(t, nodes, nil)
	if !nodes[1].IsVoter(4) || nodes[1].IsLearner(4) {
		t.Fatal("node 4 not promoted to voter")
	}
	if nodes[1].NumVoters() != before+1 {
		t.Fatalf("voter count after promote = %d, want %d", nodes[1].NumVoters(), before+1)
	}
	// Node 4 now participates: it can vote and be counted.
	for _, id := range []uint64{1, 2, 3, 4} {
		if !nodes[id].IsVoter(4) {
			t.Fatalf("node %d does not see node 4 as a voter", id)
		}
	}
}

func TestOneConfChangeAtATime(t *testing.T) {
	nodes := cluster(1, 2, 3)
	nodes[1].Campaign()
	deliver(t, nodes, nil)
	// Propose a conf change but do NOT let it apply.
	if err := nodes[1].ProposeConfChange(ConfChange{ConfAddLearner, 5}); err != nil {
		t.Fatal(err)
	}
	// A second one before the first applies is rejected.
	if err := nodes[1].ProposeConfChange(ConfChange{ConfAddLearner, 6}); err == nil {
		t.Fatal("a second in-flight conf change must be rejected")
	}
	// After the first applies, another is allowed.
	deliver(t, nodes, nil)
	if err := nodes[1].ProposeConfChange(ConfChange{ConfRemoveNode, 5}); err != nil {
		t.Fatalf("conf change after the first applied should be allowed: %v", err)
	}
}

func TestConfChangeSurvivesRestart(t *testing.T) {
	// A committed membership change must be rebuilt from the log on
	// restart.
	nodes := cluster(1, 2, 3)
	nodes[1].Campaign()
	deliver(t, nodes, nil)
	if err := nodes[1].ProposeConfChange(ConfChange{ConfAddLearner, 7}); err != nil {
		t.Fatal(err)
	}
	deliver(t, nodes, nil)
	if !nodes[1].IsLearner(7) {
		t.Fatal("node 7 not a learner before restart")
	}

	// Reconstruct node 1 from its persisted-equivalent log.
	rec := Recovered{
		HardState: nodes[1].HardStateNow(),
		Entries:   nodes[1].Entries(),
	}
	restored := RestoreNode(Config{ID: 1, Peers: []uint64{1, 2, 3}}, rec, nodes[1].Committed())
	if !restored.IsLearner(7) {
		t.Fatal("membership change lost across restart")
	}
	if restored.NumVoters() != 3 {
		t.Fatalf("restored voter count = %d, want 3", restored.NumVoters())
	}
}
