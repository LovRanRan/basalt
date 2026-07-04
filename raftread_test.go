package basalt

import (
	"fmt"
	"testing"

	"github.com/LovRanRan/basalt/internal/raft"
)

// newSMClusterLease builds a cluster whose nodes serve reads from a leader
// lease.
func newSMClusterLease(t *testing.T, ids []uint64) *smCluster {
	c := &smCluster{t: t, reps: map[uint64]*replica{}, ids: ids, down: map[uint64]bool{}}
	for _, id := range ids {
		c.reps[id] = openReplicaLease(t, t.TempDir(), id, ids, true)
	}
	return c
}

func openReplicaLease(t *testing.T, dir string, id uint64, ids []uint64, lease bool) *replica {
	r := openReplica(t, dir, id, ids)
	// Rebuild the node with LeaseRead set (openReplica made a strict one).
	rng := splitmixTest(id * 0x9e37)
	_ = r
	rec := raft.Recovered{}
	r.node = raft.RestoreNode(raft.Config{
		ID: id, Peers: ids, ElectionTick: 10, LeaseRead: lease,
		Rand: func(hi int) int { return int(rng() % uint64(hi)) },
	}, rec, r.sm.AppliedIndex())
	return r
}

// readIndex issues a linearizable read on the leader and pumps until it is
// served, returning false if leadership was lost.
func (c *smCluster) readIndex(lead uint64, id uint64) bool {
	if !c.reps[lead].node.ReadIndex(id) {
		return false
	}
	for i := 0; i < 40; i++ {
		c.pump(1)
		if c.reps[lead].reads[id] {
			return true
		}
		if c.reps[lead].node.Role() != raft.Leader {
			return false
		}
	}
	return c.reps[lead].reads[id]
}

func TestLinearizableReadReflectsPriorWrites(t *testing.T) {
	c := newSMCluster(t, []uint64{1, 2, 3})
	lead := c.waitLeader(100)

	readID := uint64(0)
	for i := 0; i < 200; i++ {
		var b Batch
		b.Put([]byte(fmt.Sprintf("key-%03d", i)), []byte(fmt.Sprintf("v-%d", i)))
		if !c.propose(lead, &Command{ClientID: 1, Seq: uint64(i + 1), Batch: b}) {
			t.Fatalf("propose %d rejected", i)
		}
		// Every 20 writes, issue a linearizable read: once it is served,
		// the engine must reflect every write committed before it.
		if i%20 == 19 {
			c.pump(2)
			if id, ok := c.leader(); ok {
				lead = id
			}
			readID++
			if !c.readIndex(lead, readID) {
				continue // leadership churned; try again next round
			}
			// The read is confirmed and applied: this key must be present.
			v, err := c.reps[lead].db.Get([]byte(fmt.Sprintf("key-%03d", i)))
			if err != nil || string(v) != fmt.Sprintf("v-%d", i) {
				t.Fatalf("linearizable read missed a prior write: key-%03d = %q, %v", i, v, err)
			}
		}
	}
	if readID == 0 {
		t.Fatal("no reads were served")
	}
	for _, id := range c.ids {
		_ = c.reps[id].db.Close()
		_ = c.reps[id].st.Close()
	}
}

func TestDeposedLeaderCannotServeReads(t *testing.T) {
	c := newSMCluster(t, []uint64{1, 2, 3})
	lead := c.waitLeader(100)
	var b Batch
	b.Put([]byte("k"), []byte("v"))
	if !c.propose(lead, &Command{ClientID: 1, Seq: 1, Batch: b}) {
		t.Fatal("propose failed")
	}
	c.pump(20)

	// Partition the leader from the majority. Its ReadIndex round can
	// never collect a quorum, so the read must NOT be served.
	c.down[lead] = true
	// Let the majority elect a new leader.
	for i := 0; i < 200; i++ {
		c.pump(1)
		lers := 0
		for _, id := range c.ids {
			if id != lead && c.reps[id].node.Role() == raft.Leader {
				lers++
			}
		}
		if lers == 1 {
			break
		}
	}
	// Revive the old leader but keep it a minority island by re-isolating
	// only its outbound acceptance: simplest is to try ReadIndex while it
	// is still down (its messages are dropped).
	old := c.reps[lead]
	if old.node.Role() == raft.Leader {
		if old.node.ReadIndex(999) {
			// Pump a few ticks with the leader still partitioned — no
			// quorum can form.
			for i := 0; i < 30; i++ {
				c.pump(1)
			}
			if old.reads[999] {
				t.Fatal("a partitioned leader served a linearizable read")
			}
		}
	}
	for _, id := range c.ids {
		_ = c.reps[id].db.Close()
		_ = c.reps[id].st.Close()
	}
}

func TestLeaseReadServesWithoutQuorumRound(t *testing.T) {
	c := newSMClusterLease(t, []uint64{1, 2, 3})
	lead := c.waitLeader(100)
	var b Batch
	b.Put([]byte("lease-key"), []byte("v"))
	if !c.propose(lead, &Command{ClientID: 1, Seq: 1, Batch: b}) {
		t.Fatal("propose failed")
	}
	c.pump(20)

	// With a fresh lease (heartbeats just flowed), a read is served
	// promptly. Correctness still holds: the value is present.
	if !c.readIndex(lead, 1) {
		t.Fatal("lease read not served")
	}
	if v, err := c.reps[lead].db.Get([]byte("lease-key")); err != nil || string(v) != "v" {
		t.Fatalf("lease read stale: %q %v", v, err)
	}
	for _, id := range c.ids {
		_ = c.reps[id].db.Close()
		_ = c.reps[id].st.Close()
	}
}
