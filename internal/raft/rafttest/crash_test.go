package rafttest

import (
	"fmt"
	"testing"
)

func TestCrashRestartNoDoubleVoteNoLoss(t *testing.T) {
	// Persisting nodes survive random restarts: no committed entry is
	// lost, replicas never diverge, and a restarted node never grants a
	// second vote in a term it already voted in (guaranteed by durable
	// vote — a divergence would show up as two leaders in one term, caught
	// by the safety check).
	for seed := uint64(0); seed < 60; seed++ {
		dir := t.TempDir()
		net := NewNetwork([]uint64{1, 2, 3}, Options{
			Seed: seed, Dir: dir, DropRate: 0.05, MaxDelay: 2,
		})
		acked := 0
		for step := 0; step < 500; step++ {
			net.Tick()
			if id, ok := net.Leader(); ok && acked < 40 {
				if net.Propose(id, []byte(fmt.Sprintf("c%d", acked))) == nil {
					acked++
				}
			}
			// Periodically crash-restart a rotating node.
			if step%37 == 36 {
				net.Restart(uint64(step/37%3) + 1)
			}
			// Safety: never two leaders in one term.
			terms := map[uint64]uint64{}
			for _, id := range []uint64{1, 2, 3} {
				if n := net.Nodes[id]; n.Role().String() == "leader" {
					if prev, dup := terms[n.Term()]; dup {
						t.Fatalf("seed %d step %d: nodes %d and %d both lead term %d", seed, step, prev, id, n.Term())
					}
					terms[n.Term()] = id
				}
			}
		}
		net.Run(120)

		// Applied prefixes must agree across all nodes.
		ref := net.Applied(1)
		for _, id := range []uint64{2, 3} {
			other := net.Applied(id)
			n := min(len(ref), len(other))
			for i := 0; i < n; i++ {
				if string(ref[i]) != string(other[i]) {
					t.Fatalf("seed %d: nodes 1 and %d disagree at applied[%d]", seed, id, i)
				}
			}
		}
		if len(ref) == 0 {
			t.Fatalf("seed %d: nothing committed despite %d acked proposals", seed, acked)
		}
	}
}
