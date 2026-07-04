//go:build chaos

package cluster

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	basalt "github.com/LovRanRan/basalt"
)

// partition symmetrically severs the raft transport between a and the rest of
// the cluster: a drops everything outbound, everyone else drops traffic to a.
// heal is the returned function.
func partition(k *killable, a uint64) (heal func()) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.nodes[a].setRaftDrop(func(uint64) bool { return true })
	for id, n := range k.nodes {
		if id == a {
			continue
		}
		n.setRaftDrop(func(peer uint64) bool { return peer == a })
	}
	return func() {
		k.mu.Lock()
		defer k.mu.Unlock()
		for _, n := range k.nodes {
			n.setRaftDrop(nil)
		}
	}
}

// proposeVia proposes a single put starting at node id, retrying transient
// errors until the deadline and following not-leader redirects — leadership
// legitimately moves under CI scheduling stalls, and pinning retries to one
// node would turn that into a spurious failure.
func proposeVia(t *testing.T, k *killable, id uint64, key, val string, seq uint64, d time.Duration) error {
	return propose(t, k, id, key, val, seq, d, true)
}

// proposePinned retries against exactly one node, never following redirects —
// for asserting that a specific node CANNOT commit (an isolated leader).
func proposePinned(t *testing.T, k *killable, id uint64, key, val string, seq uint64, d time.Duration) error {
	return propose(t, k, id, key, val, seq, d, false)
}

func propose(t *testing.T, k *killable, id uint64, key, val string, seq uint64, d time.Duration, follow bool) error {
	t.Helper()
	deadline := time.Now().Add(d)
	var err error
	for time.Now().Before(deadline) {
		k.mu.Lock()
		n := k.nodes[id]
		k.mu.Unlock()
		if n == nil {
			return fmt.Errorf("node %d not running", id)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		var b basalt.Batch
		b.Put([]byte(key), []byte(val))
		err = n.Propose(ctx, &basalt.Command{ClientID: basalt.NoDedupClient | 0xfa, Seq: seq, Batch: b})
		cancel()
		if err == nil {
			return nil
		}
		if follow {
			var nl *ErrNotLeader
			if errors.As(err, &nl) && nl.Leader != 0 {
				k.mu.Lock()
				_, ok := k.nodes[nl.Leader]
				k.mu.Unlock()
				if ok {
					id = nl.Leader
				}
			} else if l, ok := k.leader(); ok {
				id = l
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	return err
}

// waitValue waits until node id's engine holds key=val.
func waitValue(t *testing.T, k *killable, id uint64, key, val string, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		k.mu.Lock()
		n := k.nodes[id]
		k.mu.Unlock()
		if v, err := n.DB().Get([]byte(key)); err == nil && string(v) == val {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("node %d never converged to %s=%s", id, key, val)
}

// TestFaultMinorityPartitionedLeader isolates the leader on the minority side
// of a partition: the majority elects a new leader and keeps committing, the
// old leader cannot commit anything, and on heal it rejoins as a follower with
// its dead-end proposal truncated away.
func TestFaultMinorityPartitionedLeader(t *testing.T) {
	ids := []uint64{1, 2, 3}
	k := startKillable(t, ids)
	defer k.stopAll()
	old := k.waitLeader(40 * time.Second)

	// A committed write everyone has before the trouble starts.
	if err := proposeVia(t, k, old, "stable", "s1", 1, 15*time.Second); err != nil {
		t.Fatalf("baseline write: %v", err)
	}
	_, oldTerm, _ := k.nodes[old].Status()

	heal := partition(k, old)

	// The majority side elects a fresh leader and commits new writes.
	var maj []uint64
	for _, id := range ids {
		if id != old {
			maj = append(maj, id)
		}
	}
	var newLead uint64
	deadline := time.Now().Add(40 * time.Second)
	for time.Now().Before(deadline) && newLead == 0 {
		for _, id := range maj {
			if _, _, isLeader := k.nodes[id].Status(); isLeader {
				newLead = id
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if newLead == 0 {
		t.Fatal("majority never elected a leader while the old leader was partitioned")
	}
	if err := proposeVia(t, k, newLead, "during", "d1", 2, 15*time.Second); err != nil {
		t.Fatalf("majority write during partition: %v", err)
	}

	// The isolated old leader cannot commit: a proposal through it must fail
	// (context expiry or a stepdown-driven not-leader), and the value must
	// never become durable anywhere. Pinned: the whole point is that THIS
	// node cannot commit.
	if err := proposePinned(t, k, old, "orphan", "o1", 3, 3*time.Second); err == nil {
		t.Fatal("isolated minority leader committed a write")
	}

	heal()

	// The old leader converges: it has the majority's write, and its term
	// advanced past its partition-era term (it cannot still be leading at the
	// old term; whether it later wins a fresh, legitimate election is raft's
	// business, so leadership itself is not asserted).
	waitValue(t, k, old, "during", "d1", 20*time.Second)
	rejoin := time.Now().Add(20 * time.Second)
	termAdvanced := false
	for time.Now().Before(rejoin) && !termAdvanced {
		if _, term, _ := k.nodes[old].Status(); term > oldTerm {
			termAdvanced = true
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !termAdvanced {
		t.Fatalf("old leader never advanced past its partition-era term %d", oldTerm)
	}
	for _, id := range ids {
		if _, err := k.nodes[id].DB().Get([]byte("orphan")); !errors.Is(err, basalt.ErrNotFound) {
			t.Fatalf("node %d holds the isolated leader's uncommitted write: %v", id, err)
		}
	}
}

// TestFaultRepeatedLeaderKillWithRestart kills the leader, lets the majority
// take over, restarts the killed node, and repeats — acked writes must survive
// every cycle and the reborn nodes must catch back up.
func TestFaultRepeatedLeaderKillWithRestart(t *testing.T) {
	ids := []uint64{1, 2, 3}
	k := startKillable(t, ids)
	defer k.stopAll()
	k.waitLeader(20 * time.Second)

	seq := uint64(0)
	for cycle := 0; cycle < 2; cycle++ {
		lead := k.waitLeader(40 * time.Second)
		seq++
		key := fmt.Sprintf("cycle-%d", cycle)
		if err := proposeVia(t, k, lead, key, "acked", seq, 15*time.Second); err != nil {
			t.Fatalf("cycle %d write: %v", cycle, err)
		}
		k.kill(lead)
		next := k.waitLeader(40 * time.Second) // majority re-elects
		seq++
		if err := proposeVia(t, k, next, key+"-post", "acked", seq, 15*time.Second); err != nil {
			t.Fatalf("cycle %d post-kill write: %v", cycle, err)
		}
		k.boot(lead) // the dead node comes back and must catch up
		waitValue(t, k, lead, key+"-post", "acked", 30*time.Second)
	}

	// Every acked write from every cycle is present on every node.
	for _, id := range ids {
		for cycle := 0; cycle < 2; cycle++ {
			waitValue(t, k, id, fmt.Sprintf("cycle-%d", cycle), "acked", 20*time.Second)
			waitValue(t, k, id, fmt.Sprintf("cycle-%d-post", cycle), "acked", 20*time.Second)
		}
	}
}

// TestFaultSlowDiskFollower gives one node a crippled disk: quorum comes from
// the two healthy nodes so writes stay fast, and the slow node still converges
// to the full state, just late.
func TestFaultSlowDiskFollower(t *testing.T) {
	ids := []uint64{1, 2, 3}
	const slow = uint64(3)
	k := &killable{
		t: t, ids: ids,
		peers: map[uint64]string{}, kvAddr: map[uint64]string{},
		dataDirs: map[uint64]string{}, nodes: map[uint64]*Node{}, srv: map[uint64]*Servers{},
	}
	for _, id := range ids {
		rl, ra := freePort(t)
		kl, ka := freePort(t)
		_ = rl.Close()
		_ = kl.Close()
		k.peers[id], k.kvAddr[id] = ra, ka
		k.dataDirs[id] = t.TempDir()
	}
	for _, id := range ids {
		cfg := Config{
			ID: id, Peers: k.peers, DataDir: k.dataDirs[id],
			ElectionTick: 10, HeartbeatTick: 1, TickInterval: 10 * time.Millisecond,
		}
		if id == slow {
			cfg.fsyncDelay = 30 * time.Millisecond // ~3 ticks per persistence stall
		}
		n, err := Open(cfg)
		if err != nil {
			t.Fatal(err)
		}
		rl, err := net.Listen("tcp", k.peers[id])
		if err != nil {
			t.Fatal(err)
		}
		kl, err := net.Listen("tcp", k.kvAddr[id])
		if err != nil {
			t.Fatal(err)
		}
		k.nodes[id] = n
		k.srv[id] = n.Serve(rl, kl)
	}
	defer k.stopAll()

	lead := k.waitLeader(20 * time.Second)
	if lead == slow {
		// Move leadership onto a healthy node so the slow disk is a follower
		// problem, which is the scenario under test.
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		target := ids[0]
		if target == slow {
			target = ids[1]
		}
		if err := k.nodes[lead].only().TransferLeader(ctx, target); err != nil {
			cancel()
			t.Fatalf("transfer off slow node: %v", err)
		}
		cancel()
		deadline := time.Now().Add(15 * time.Second)
		for time.Now().Before(deadline) {
			if l, ok := k.leader(); ok && l != slow {
				lead = l
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		if lead == slow {
			t.Fatal("leadership never moved off the slow-disk node")
		}
	}

	// 40 sequential writes through the healthy quorum: with the slow node out
	// of the critical path they must complete promptly (each write would cost
	// >=30ms if the slow disk gated the quorum; 40 of them well under that
	// worst case proves it does not).
	start := time.Now()
	for i := 0; i < 40; i++ {
		if err := proposeVia(t, k, lead, fmt.Sprintf("sd-%02d", i), "v", uint64(i+1), 15*time.Second); err != nil {
			t.Fatalf("write %d with a slow follower: %v", i, err)
		}
	}
	healthyDur := time.Since(start)
	t.Logf("40 writes with a slow follower took %v", healthyDur)
	// If the slow disk gated the quorum, each write would cost >= the 30ms
	// injected stall: 40 writes >= 1.2s of injected sleep alone. Healthy
	// loopback commits are single-digit ms, so this bound has wide margin.
	if healthyDur >= 40*30*time.Millisecond {
		t.Fatalf("40 writes took %v; the slow follower appears to gate the quorum", healthyDur)
	}

	// The slow follower still converges to every write, just late.
	waitValue(t, k, slow, "sd-39", "v", 30*time.Second)
}
