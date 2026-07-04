package basalt

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// snapshotAndCompact runs the full local-snapshot protocol on one replica:
// engine checkpoint → compact the in-memory log → rewrite the raft storage
// so the disk footprint shrinks with it.
func snapshotAndCompact(t *testing.T, r *replica, dst string) (uint64, uint64) {
	t.Helper()
	index, term, err := r.sm.Snapshot(dst)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	r.node.CompactTo(index)
	if err := r.st.Rewrite(r.node.HardStateNow(), r.node.SnapIndex(), r.node.SnapTerm(), r.node.Entries()); err != nil {
		t.Fatalf("rewrite: %v", err)
	}
	return index, term
}

func raftFileSize(t *testing.T, r *replica) int64 {
	t.Helper()
	st, err := os.Stat(filepath.Join(r.dir, "raft", "state"))
	if err != nil {
		t.Fatal(err)
	}
	return st.Size()
}

func TestSnapshotKeepsRaftLogBounded(t *testing.T) {
	c := newSMCluster(t, []uint64{1, 2, 3})
	lead := c.waitLeader(100)

	var peak int64
	snapshots := 0
	for i := 0; i < 3000; i++ {
		var b Batch
		b.Put([]byte(fmt.Sprintf("key-%05d", i%500)), []byte(fmt.Sprintf("v-%d", i)))
		if !c.propose(lead, &Command{ClientID: 1, Seq: uint64(i + 1), Batch: b}) {
			t.Fatalf("propose %d rejected", i)
		}
		if i%50 == 0 {
			c.pump(1)
			if id, ok := c.leader(); ok {
				lead = id
			}
		}
		// Every 400 commands, each replica snapshots and compacts.
		if i%400 == 399 {
			c.pump(5)
			for _, id := range c.ids {
				snapDir := filepath.Join(t.TempDir(), fmt.Sprintf("snap-%d-%d", id, i))
				snapshotAndCompact(t, c.reps[id], snapDir)
				snapshots++
			}
			for _, id := range c.ids {
				if sz := raftFileSize(t, c.reps[id]); sz > peak {
					peak = sz
				}
			}
		}
	}
	c.pump(30)
	if snapshots == 0 {
		t.Fatal("no snapshots taken")
	}
	// The raft log on disk stays bounded: after 3000 commands with
	// compaction every 400, no state file may hold anything near the full
	// history (a full history at ~60B/command would exceed 180KB).
	for _, id := range c.ids {
		if sz := raftFileSize(t, c.reps[id]); sz > 100<<10 {
			t.Fatalf("replica %d raft state is %dB — log not bounded", id, sz)
		}
	}
	// And the replicas still agree.
	ref := scanState(t, c.reps[1].db)
	for _, id := range []uint64{2, 3} {
		got := scanState(t, c.reps[id].db)
		if len(got) != len(ref) {
			t.Fatalf("replica %d has %d keys, want %d", id, len(got), len(ref))
		}
	}
	for _, id := range c.ids {
		_ = c.reps[id].db.Close()
		_ = c.reps[id].st.Close()
	}
}

func TestRestartFromSnapshotPlusSuffix(t *testing.T) {
	c := newSMCluster(t, []uint64{1, 2, 3})
	lead := c.waitLeader(100)

	propose := func(from, to int) {
		t.Helper()
		for i := from; i < to; i++ {
			var b Batch
			b.Put([]byte(fmt.Sprintf("key-%04d", i)), []byte(fmt.Sprintf("v-%d", i)))
			if !c.propose(lead, &Command{ClientID: 1, Seq: uint64(i + 1), Batch: b}) {
				t.Fatalf("propose %d rejected", i)
			}
			if i%40 == 0 {
				c.pump(1)
				if id, ok := c.leader(); ok {
					lead = id
				}
			}
		}
		c.pump(20)
	}

	propose(0, 500)
	// Every replica snapshots at ~500 and compacts its log.
	snapIdx := uint64(0)
	snapDir1 := filepath.Join(t.TempDir(), "snap-1")
	for _, id := range c.ids {
		dst := filepath.Join(t.TempDir(), fmt.Sprintf("snap-%d", id))
		if id == 1 {
			dst = snapDir1
		}
		idx, term := snapshotAndCompact(t, c.reps[id], dst)
		if idx == 0 || term == 0 {
			t.Fatalf("replica %d snapshot meta = (%d, %d)", id, idx, term)
		}
		if id == 1 {
			snapIdx = idx
		}
	}
	// More traffic lands AFTER the snapshot boundary.
	propose(500, 900)

	// Crash-restart a follower: it must come back from snapshot boundary +
	// persisted suffix, replay the committed tail into its engine, and
	// converge.
	victim := uint64(2)
	if victim == lead {
		victim = 3
	}
	c.crashRestart(victim)
	r := c.reps[victim]
	if r.node.SnapIndex() == 0 {
		t.Fatal("restarted node lost its snapshot boundary")
	}
	if r.node.SnapIndex() < snapIdx {
		t.Fatalf("restarted snap index %d below %d", r.node.SnapIndex(), snapIdx)
	}
	c.pump(60)

	ref := scanState(t, c.reps[1].db)
	got := scanState(t, r.db)
	if len(got) != len(ref) {
		t.Fatalf("restarted replica has %d keys, want %d", len(got), len(ref))
	}
	for k, v := range ref {
		if got[k] != v {
			t.Fatalf("restarted replica: %q = %q, want %q", k, got[k], v)
		}
	}
	// The snapshot directory itself opens as a valid point-in-time DB whose
	// reserved metadata names the raft prefix it covers.
	idx, term, err := ReadSnapshotMeta(snapDir1)
	if err != nil || idx != snapIdx || term == 0 {
		t.Fatalf("snapshot meta = (%d, %d, %v), want index %d", idx, term, err, snapIdx)
	}
	// A dir that never was a snapshot reads back as empty (index 0).
	if idx, _, err := ReadSnapshotMeta(filepath.Join(t.TempDir(), "fresh")); err != nil || idx != 0 {
		t.Fatalf("non-snapshot dir meta = (%d, %v), want (0, nil)", idx, err)
	}
	for _, id := range c.ids {
		_ = c.reps[id].db.Close()
		_ = c.reps[id].st.Close()
	}
}

func TestCompactBeyondAppliedPanics(t *testing.T) {
	c := newSMCluster(t, []uint64{1, 2, 3})
	lead := c.waitLeader(100)
	var b Batch
	b.Put([]byte("k"), []byte("v"))
	if !c.propose(lead, &Command{ClientID: 1, Seq: 1, Batch: b}) {
		t.Fatal("propose failed")
	}
	c.pump(20)
	defer func() {
		if recover() == nil {
			t.Fatal("compacting beyond applied must panic")
		}
		for _, id := range c.ids {
			_ = c.reps[id].db.Close()
			_ = c.reps[id].st.Close()
		}
	}()
	c.reps[lead].node.CompactTo(1 << 40)
}
