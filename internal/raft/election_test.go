package raft

import "testing"

// stepUntilQuiet delivers messages between nodes until none remain,
// applying term rules through Step. Purely in-memory and deterministic.
func deliver(t *testing.T, nodes map[uint64]*Node, drop func(m Message) bool) {
	t.Helper()
	for round := 0; round < 1000; round++ {
		var pending []Message
		for _, n := range nodes {
			rd := n.Ready()
			pending = append(pending, rd.Messages...)
			n.Advance(rd)
		}
		if len(pending) == 0 {
			return
		}
		for _, m := range pending {
			if drop != nil && drop(m) {
				continue
			}
			if err := nodes[m.To].Step(m); err != nil {
				t.Fatalf("step: %v", err)
			}
		}
	}
	t.Fatal("delivery never quiesced")
}

func cluster(ids ...uint64) map[uint64]*Node {
	nodes := map[uint64]*Node{}
	for _, id := range ids {
		nodes[id] = NewNode(id, ids)
	}
	return nodes
}

func leaders(nodes map[uint64]*Node) []uint64 {
	var out []uint64
	for id, n := range nodes {
		if n.Role() == Leader {
			out = append(out, id)
		}
	}
	return out
}

func TestElectionElectsOneLeader(t *testing.T) {
	nodes := cluster(1, 2, 3)
	nodes[1].Campaign()
	deliver(t, nodes, nil)

	if got := leaders(nodes); len(got) != 1 || got[0] != 1 {
		t.Fatalf("leaders = %v, want [1]", got)
	}
	for id, n := range nodes {
		if n.Term() != 1 {
			t.Fatalf("node %d term = %d, want 1", id, n.Term())
		}
		if n.Lead() != 1 {
			t.Fatalf("node %d lead = %d, want 1", id, n.Lead())
		}
	}
}

func TestVoteOncePerTerm(t *testing.T) {
	// Two candidates campaign in the same term; each follower may grant
	// only one, so at most one wins.
	nodes := cluster(1, 2, 3)
	nodes[1].Campaign()
	nodes[2].Campaign()
	deliver(t, nodes, nil)
	if got := leaders(nodes); len(got) > 1 {
		t.Fatalf("two leaders in one term: %v", got)
	}
}

func TestPreVoteDoesNotDisruptHealthyLeader(t *testing.T) {
	nodes := cluster(1, 2, 3)
	nodes[1].Campaign()
	deliver(t, nodes, nil)
	if nodes[1].Role() != Leader {
		t.Fatal("setup: node 1 should lead")
	}
	leaderTerm := nodes[1].Term()

	// Node 3 is partitioned and times out repeatedly: without PreVote it
	// would ratchet its term up and depose the leader on heal. With
	// PreVote, isolated campaigns cannot raise the term.
	for i := 0; i < 5; i++ {
		nodes[3].Campaign()
		// Deliver only node 3's own messages into the void (partition:
		// drop everything to/from it).
		deliver(t, nodes, func(m Message) bool { return m.To == 3 || m.From == 3 })
	}
	if nodes[3].Term() > leaderTerm {
		t.Fatalf("partitioned node raised its term to %d (leader at %d) — prevote failed", nodes[3].Term(), leaderTerm)
	}

	// Heal: the leader's heartbeat brings node 3 back in line, no election.
	nodes[1].Tick()
	deliver(t, nodes, nil)
	if nodes[1].Role() != Leader || nodes[1].Term() != leaderTerm {
		t.Fatalf("leader disrupted after heal: role=%v term=%d", nodes[1].Role(), nodes[1].Term())
	}
}

func TestElectionTimeoutIsRandomizedAndBounded(t *testing.T) {
	timeouts := map[int]bool{}
	for seed := int64(0); seed < 50; seed++ {
		r := newSplitmix(uint64(seed))
		n := NewNodeConfig(Config{ID: 1, Peers: []uint64{1, 2, 3}, ElectionTick: 10, Rand: func(hi int) int { return int(r() % uint64(hi)) }})
		ticks := 0
		for n.Role() == Follower {
			n.Tick()
			ticks++
			if ticks > 100 {
				t.Fatal("election never fired")
			}
		}
		if ticks < 10 || ticks >= 20 {
			t.Fatalf("timeout %d outside [10, 20)", ticks)
		}
		timeouts[ticks] = true
	}
	if len(timeouts) < 3 {
		t.Fatalf("timeouts not randomized: only %d distinct values", len(timeouts))
	}
}

func TestLeaderStepsDownOnHigherTerm(t *testing.T) {
	nodes := cluster(1, 2, 3)
	nodes[1].Campaign()
	deliver(t, nodes, nil)
	if nodes[1].Role() != Leader {
		t.Fatal("node 1 should lead")
	}
	// A message from a strictly higher term demotes the leader.
	if err := nodes[1].Step(Message{Type: MsgApp, From: 2, Term: 9}); err != nil {
		t.Fatal(err)
	}
	if nodes[1].Role() != Follower || nodes[1].Term() != 9 {
		t.Fatalf("leader did not step down: role=%v term=%d", nodes[1].Role(), nodes[1].Term())
	}
}

func TestSeededElectionSafety(t *testing.T) {
	// Across many seeds, concurrent campaigns never elect two leaders in
	// one term.
	for seed := int64(0); seed < 200; seed++ {
		nodes := cluster(1, 2, 3, 4, 5)
		r := newSplitmix(uint64(seed))
		for _, n := range nodes {
			n.rand = func(hi int) int { return int(r() % uint64(hi)) }
		}
		// A random subset campaigns.
		for id := uint64(1); id <= 5; id++ {
			if r()%2 == 0 {
				nodes[id].Campaign()
			}
		}
		deliver(t, nodes, nil)
		termLeaders := map[uint64]uint64{}
		for id, n := range nodes {
			if n.Role() == Leader {
				if prev, ok := termLeaders[n.Term()]; ok {
					t.Fatalf("seed %d: leaders %d and %d both at term %d", seed, prev, id, n.Term())
				}
				termLeaders[n.Term()] = id
			}
		}
	}
}

func newSplitmix(seed uint64) func() uint64 {
	s := seed
	return func() uint64 {
		s += 0x9e3779b97f4a7c15
		z := s
		z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
		z = (z ^ (z >> 27)) * 0x94d049bb133111eb
		return z ^ (z >> 31)
	}
}
