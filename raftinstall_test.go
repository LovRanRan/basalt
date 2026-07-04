package basalt

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
)

// copyTreeChunked copies src into a fresh dst, streaming each file in small
// chunks — the offset/done framing an InstallSnapshot RPC would use.
func copyTreeChunked(dst, src string) error {
	if err := os.MkdirAll(dst, 0o755); err != nil {
		return err
	}
	entries, err := os.ReadDir(src)
	if err != nil {
		return err
	}
	for _, e := range entries {
		sp, dp := filepath.Join(src, e.Name()), filepath.Join(dst, e.Name())
		if e.IsDir() {
			if err := copyTreeChunked(dp, sp); err != nil {
				return err
			}
			continue
		}
		if err := copyFileChunked(dp, sp); err != nil {
			return err
		}
	}
	return nil
}

func copyFileChunked(dst, src string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer func() { _ = out.Close() }()
	buf := make([]byte, 4096) // small chunk on purpose
	for {
		n, rerr := in.Read(buf)
		if n > 0 {
			if _, werr := out.Write(buf[:n]); werr != nil {
				return werr
			}
		}
		if rerr == io.EOF {
			return nil
		}
		if rerr != nil {
			return rerr
		}
	}
}

func TestInstallSnapshotCatchesUpACompactedFollower(t *testing.T) {
	c := newSMCluster(t, []uint64{1, 2, 3})
	lead := c.waitLeader(100)

	victim := uint64(2)
	if victim == lead {
		victim = 3
	}
	// Isolate the follower, then push a lot of traffic and compact the
	// leader's log well past what the follower has — so a plain
	// AppendEntries can no longer reach it and the leader must ship a
	// snapshot.
	c.down[victim] = true
	for i := 0; i < 1200; i++ {
		var b Batch
		b.Put([]byte(fmt.Sprintf("key-%04d", i%400)), []byte(fmt.Sprintf("v-%d", i)))
		if !c.propose(lead, &Command{ClientID: 1, Seq: uint64(i + 1), Batch: b}) {
			// Leader may change among the two live nodes; find the new one.
			c.pump(2)
			if id, ok := c.leader(); ok {
				lead = id
			}
			continue
		}
		if i%30 == 0 {
			c.pump(1)
			if id, ok := c.leader(); ok {
				lead = id
			}
		}
	}
	c.pump(10)
	// Compact every live node's log past the follower's position.
	for _, id := range c.ids {
		if id == victim {
			continue
		}
		snapshotAndCompact(t, c.reps[id], filepath.Join(t.TempDir(), fmt.Sprintf("snap-%d", id)))
	}

	// Heal the follower: catching it up now requires InstallSnapshot,
	// because the leader compacted away the entries it lacks.
	delete(c.down, victim)
	before := c.snaps
	c.pump(120)
	if c.snaps <= before {
		t.Fatal("no snapshot install happened — follower should have needed one")
	}

	// The recovered follower converges to the leader's state.
	ref := scanState(t, c.reps[lead].db)
	got := scanState(t, c.reps[victim].db)
	if len(got) != len(ref) {
		t.Fatalf("caught-up follower has %d keys, want %d", len(got), len(ref))
	}
	for k, v := range ref {
		if got[k] != v {
			t.Fatalf("follower: %q = %q, want %q", k, got[k], v)
		}
	}
	// It then keeps up with new traffic normally.
	for i := 1200; i < 1300; i++ {
		var b Batch
		b.Put([]byte(fmt.Sprintf("late-%04d", i)), []byte("late"))
		if id, ok := c.leader(); ok {
			lead = id
		}
		_ = c.propose(lead, &Command{ClientID: 1, Seq: uint64(i + 1), Batch: b})
		if i%20 == 0 {
			c.pump(1)
		}
	}
	c.pump(40)
	if v, err := c.reps[victim].db.Get([]byte("late-1299")); err != nil || string(v) != "late" {
		t.Fatalf("follower did not keep up post-install: %q %v", v, err)
	}

	for _, id := range c.ids {
		_ = c.reps[id].db.Close()
		_ = c.reps[id].st.Close()
	}
}

func TestRestoreSnapshotInMemory(t *testing.T) {
	// The in-memory RestoreSnapshot path (no reopen): a follower jumps its
	// log to a boundary and acknowledges the leader.
	c := newSMCluster(t, []uint64{1, 2, 3})
	lead := c.waitLeader(100)
	for i := 0; i < 40; i++ {
		var b Batch
		b.Put([]byte(fmt.Sprintf("k%02d", i)), []byte("v"))
		_ = c.propose(lead, &Command{ClientID: 1, Seq: uint64(i + 1), Batch: b})
		if i%10 == 0 {
			c.pump(1)
			if id, ok := c.leader(); ok {
				lead = id
			}
		}
	}
	c.pump(20)
	follower := uint64(2)
	if follower == lead {
		follower = 3
	}
	fn := c.reps[follower].node
	committedBefore := fn.Committed()
	// A stale snapshot (below what we already have) is refused.
	if fn.RestoreSnapshot(1, 1) {
		t.Fatal("stale snapshot must be refused")
	}
	if fn.Committed() != committedBefore {
		t.Fatal("refused snapshot changed state")
	}
	for _, id := range c.ids {
		_ = c.reps[id].db.Close()
		_ = c.reps[id].st.Close()
	}
}
