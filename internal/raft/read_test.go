package raft

import "testing"

func TestReadIndexRequiresLeaderAndCurrentTerm(t *testing.T) {
	n := NewNode(1, []uint64{1, 2, 3})
	if n.ReadIndex(1) {
		t.Fatal("follower must reject ReadIndex")
	}
	// A single-node cluster self-elects and commits its no-op immediately.
	s := NewNode(1, []uint64{1})
	s.Campaign()
	drainRead(t, s)
	if !s.ReadIndex(7) {
		t.Fatal("leader with a committed current-term entry must accept ReadIndex")
	}
	// The read surfaces once applied.
	rd := s.Ready()
	found := false
	for _, r := range rd.ReadStates {
		if r.ID == 7 {
			found = true
		}
	}
	s.Advance(rd)
	if !found {
		// It may need one more Ready cycle for apply to catch up.
		for s.HasReady() {
			rd = s.Ready()
			for _, r := range rd.ReadStates {
				if r.ID == 7 {
					found = true
				}
			}
			s.Advance(rd)
		}
	}
	if !found {
		t.Fatal("single-node ReadIndex never surfaced")
	}
}

func TestReadIndexNeedsQuorumConfirmation(t *testing.T) {
	nodes := cluster(1, 2, 3)
	nodes[1].Campaign()
	deliver(t, nodes, nil)
	lead := nodes[1]
	if lead.Role() != Leader {
		t.Fatal("node 1 should lead")
	}
	if !lead.ReadIndex(42) {
		t.Fatal("leader rejected ReadIndex")
	}
	// Drop every message: the confirmation heartbeats never reach a
	// quorum, so the read stays unconfirmed no matter how many ticks pass.
	for i := 0; i < 5; i++ {
		lead.Tick()
		deliver(t, nodes, func(Message) bool { return true })
	}
	for _, r := range lead.Ready().ReadStates {
		if r.ID == 42 {
			t.Fatal("read served without quorum confirmation")
		}
	}
	// Let messages flow and tick: the next heartbeat round confirms it.
	// Track served reads via a delivery loop that records ReadStates
	// before advancing (deliver() would consume them).
	served := false
	record := func() {
		for _, id := range []uint64{1, 2, 3} {
			for nodes[id].HasReady() {
				rd := nodes[id].Ready()
				for _, r := range rd.ReadStates {
					if r.ID == 42 {
						served = true
					}
				}
				nodes[id].Advance(rd)
				for _, m := range rd.Messages {
					_ = nodes[m.To].Step(m)
				}
			}
		}
	}
	for i := 0; i < 6 && !served; i++ {
		lead.Tick()
		record()
	}
	if !served {
		t.Fatal("read never confirmed after messages flowed")
	}
}

func drainRead(t *testing.T, n *Node) {
	t.Helper()
	for i := 0; n.HasReady(); i++ {
		if i > 100 {
			t.Fatal("ready loop stuck")
		}
		rd := n.Ready()
		n.Advance(rd)
	}
}
