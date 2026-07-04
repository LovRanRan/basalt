package basalt

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/LovRanRan/basalt/internal/raft"
)

func splitmixTest(seed uint64) func() uint64 {
	s := seed
	return func() uint64 {
		s += 0x9e3779b97f4a7c15
		z := s
		z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
		z = (z ^ (z >> 27)) * 0x94d049bb133111eb
		return z ^ (z >> 31)
	}
}

// replica is one member: raft node + storage + WAL-less engine + state
// machine, glued by a test pump.
type replica struct {
	id    uint64
	node  *raft.Node
	st    *raft.Storage
	db    *DB
	sm    *StateMachine
	dir   string
	reads map[uint64]bool // read ids that surfaced as served (linearizable)
}

type smCluster struct {
	t     *testing.T
	reps  map[uint64]*replica
	ids   []uint64
	queue []raft.Message
	down  map[uint64]bool
	snaps int // count of InstallSnapshot transfers performed
}

func openReplica(t *testing.T, dir string, id uint64, ids []uint64) *replica {
	t.Helper()
	db, err := Open(filepath.Join(dir, "db"), Options{
		MemTableSize: 16 << 10, // frequent flushes: recovery must cross flush boundaries
		DisableWAL:   true,
	})
	if err != nil {
		t.Fatal(err)
	}
	sm, err := NewStateMachine(db)
	if err != nil {
		t.Fatal(err)
	}
	st, rec, err := raft.OpenStorage(filepath.Join(dir, "raft", "state"))
	if err != nil {
		t.Fatal(err)
	}
	// Distinct per-node RNG seeds: identical timeout sequences would
	// split votes forever.
	rng := splitmixTest(id * 0x9e37)
	node := raft.RestoreNode(raft.Config{
		ID: id, Peers: ids, ElectionTick: 10,
		Rand: func(hi int) int { return int(rng() % uint64(hi)) },
	}, rec, sm.AppliedIndex())
	return &replica{id: id, node: node, st: st, db: db, sm: sm, dir: dir, reads: map[uint64]bool{}}
}

func newSMCluster(t *testing.T, ids []uint64) *smCluster {
	c := &smCluster{t: t, reps: map[uint64]*replica{}, ids: ids, down: map[uint64]bool{}}
	for _, id := range ids {
		c.reps[id] = openReplica(t, t.TempDir(), id, ids)
	}
	return c
}

// pump runs ticks: each tick advances every live node, persists+applies its
// Ready, and delivers queued messages.
func (c *smCluster) pump(ticks int) {
	t := c.t
	for i := 0; i < ticks; i++ {
		for _, id := range c.ids {
			if !c.down[id] {
				c.reps[id].node.Tick()
			}
		}
		for pass := 0; pass < 50; pass++ {
			moved := false
			for _, id := range c.ids {
				r := c.reps[id]
				if c.down[id] || !r.node.HasReady() {
					continue
				}
				moved = true
				rd := r.node.Ready()
				if err := r.st.SaveReady(rd); err != nil {
					t.Fatal(err)
				}
				if _, err := r.sm.Apply(rd.CommittedEntries); err != nil {
					t.Fatal(err)
				}
				for _, rs := range rd.ReadStates {
					r.reads[rs.ID] = true
				}
				c.queue = append(c.queue, rd.Messages...)
				r.node.Advance(rd)
			}
			q := c.queue
			c.queue = nil
			for _, m := range q {
				if c.down[m.To] || c.down[m.From] {
					continue
				}
				moved = true
				if m.Type == raft.MsgSnap {
					c.installSnapshot(m)
					continue
				}
				if err := c.reps[m.To].node.Step(m); err != nil && !errors.Is(err, raft.ErrNotLeader) {
					t.Fatal(err)
				}
			}
			if !moved {
				break
			}
		}
	}
}

// installSnapshot performs the out-of-band snapshot transfer a MsgSnap
// requests: the leader checkpoints its state machine, the bytes ship in
// chunks (here a file-by-file copy modeling offset/done framing), and the
// receiver closes its engine, swaps the checkpoint in as its data dir,
// resets its raft storage to the snapshot boundary (preserving its durable
// vote), and reopens. The leader's next AppendEntries then replicates the
// tail past the boundary normally.
func (c *smCluster) installSnapshot(m raft.Message) {
	t := c.t
	leader, follower := c.reps[m.From], c.reps[m.To]

	staging := filepath.Join(t.TempDir(), fmt.Sprintf("stage-%d-to-%d", m.From, m.To))
	index, term, err := leader.sm.Snapshot(staging)
	if err != nil {
		t.Fatalf("leader snapshot: %v", err)
	}
	recvDir := filepath.Join(t.TempDir(), fmt.Sprintf("recv-%d", m.To))
	if err := copyTreeChunked(recvDir, staging); err != nil {
		t.Fatalf("chunked transfer: %v", err)
	}
	got, gotTerm, err := ReadSnapshotMeta(recvDir)
	if err != nil || got != index || gotTerm != term {
		t.Fatalf("received snapshot meta = (%d,%d,%v), want (%d,%d)", got, gotTerm, err, index, term)
	}

	hs := follower.node.HardStateNow()
	newTerm := hs.Term
	newVote := hs.Vote
	if m.Term > newTerm {
		newTerm, newVote = m.Term, 0
	}

	dbDir := filepath.Join(follower.dir, "db")
	raftPath := filepath.Join(follower.dir, "raft", "state")
	crash(follower.db)
	_ = follower.st.Close()
	if err := ReplaceWithCheckpoint(recvDir, dbDir); err != nil {
		t.Fatalf("replace: %v", err)
	}
	// Reset the raft log to the boundary the engine now carries.
	st2, _, err := raft.OpenStorage(raftPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := st2.Rewrite(raft.HardState{Term: newTerm, Vote: newVote, Commit: index}, index, term, nil); err != nil {
		t.Fatal(err)
	}
	_ = st2.Close()

	c.reps[m.To] = openReplica(t, follower.dir, m.To, c.ids)
	if got := c.reps[m.To].sm.AppliedIndex(); got != index {
		t.Fatalf("post-install applied index = %d, want %d", got, index)
	}
	c.snaps++
}

func (c *smCluster) leader() (uint64, bool) {
	var lead uint64
	n := 0
	for _, id := range c.ids {
		if !c.down[id] && c.reps[id].node.Role() == raft.Leader {
			lead = id
			n++
		}
	}
	return lead, n == 1
}

func (c *smCluster) waitLeader(budget int) uint64 {
	for i := 0; i < budget; i++ {
		c.pump(1)
		if id, ok := c.leader(); ok {
			return id
		}
	}
	c.t.Fatal("no leader within budget")
	return 0
}

// crashRestart closes a replica's engine and raft storage and reopens both
// from disk — the full P3.6 recovery path.
func (c *smCluster) crashRestart(id uint64) {
	r := c.reps[id]
	crash(r.db) // release the flock the way a process death would
	_ = r.st.Close()
	c.reps[id] = openReplica(c.t, r.dir, id, c.ids)
}

func (c *smCluster) propose(lead uint64, cmd *Command) bool {
	return c.reps[lead].node.Propose(EncodeCommand(cmd)) == nil
}

// scanState collects every user-visible key/value of a replica.
func scanState(t *testing.T, db *DB) map[string]string {
	t.Helper()
	it := db.Scan(nil, nil)
	out := map[string]string{}
	for ; it.Valid(); it.Next() {
		out[string(it.Key())] = string(it.Value())
	}
	if err := it.Error(); err != nil {
		t.Fatal(err)
	}
	it.Close()
	return out
}

func TestReplicatedStateMachineConvergence(t *testing.T) {
	c := newSMCluster(t, []uint64{1, 2, 3})
	c.waitLeader(100)
	var lead uint64

	const total = 10_000
	acked := 0
	seq := map[uint64]uint64{} // per client
	kills := 0
	for step := 0; acked < total && step < 100_000; step++ {
		id, ok := c.leader()
		if !ok {
			c.pump(1)
			continue
		}
		lead = id
		// Two forced leader changes at 1/3 and 2/3 of the workload.
		if (acked == total/3 && kills == 0) || (acked == 2*total/3 && kills == 1) {
			kills++
			c.down[lead] = true
			c.pump(30)
			delete(c.down, lead)
			continue
		}
		client := uint64(acked%4 + 1)
		seq[client]++
		var b Batch
		b.Put([]byte(fmt.Sprintf("key-%05d", acked%3000)), []byte(fmt.Sprintf("v-%d", acked)))
		if acked%7 == 3 {
			b.Delete([]byte(fmt.Sprintf("key-%05d", (acked+1500)%3000)))
		}
		if c.propose(lead, &Command{ClientID: client, Seq: seq[client], Batch: b}) {
			acked++
		}
		if acked%50 == 0 {
			c.pump(1)
		}
	}
	if acked < total {
		t.Fatalf("only %d/%d proposals acked", acked, total)
	}
	if kills != 2 {
		t.Fatalf("forced %d leader changes, want 2", kills)
	}
	c.pump(60)

	ref := scanState(t, c.reps[1].db)
	if len(ref) == 0 {
		t.Fatal("replica 1 is empty")
	}
	for _, id := range []uint64{2, 3} {
		got := scanState(t, c.reps[id].db)
		if len(got) != len(ref) {
			t.Fatalf("replica %d has %d keys, replica 1 has %d", id, len(got), len(ref))
		}
		for k, v := range ref {
			if got[k] != v {
				t.Fatalf("replica %d: %q = %q, want %q", id, k, got[k], v)
			}
		}
	}
	for _, id := range c.ids {
		if a := c.reps[id].sm.AppliedIndex(); a == 0 {
			t.Fatalf("replica %d applied index is zero", id)
		}
		_ = c.reps[id].db.Close()
		_ = c.reps[id].st.Close()
	}
}

func TestCrashRestartRecoversExactPrefix(t *testing.T) {
	c := newSMCluster(t, []uint64{1, 2, 3})
	lead := c.waitLeader(100)
	for i := 0; i < 800; i++ {
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
	c.pump(40)

	// Crash-restart a follower: its engine lost everything unflushed (no
	// WAL), and raft must replay exactly from the recovered applied index.
	victim := uint64(2)
	if victim == lead {
		victim = 3
	}
	before := c.reps[victim].sm.AppliedIndex()
	c.crashRestart(victim)
	after := c.reps[victim].sm.AppliedIndex()
	if after > before {
		t.Fatalf("applied index went forward across a crash: %d -> %d", before, after)
	}
	c.pump(60)

	refState := scanState(t, c.reps[1].db)
	gotState := scanState(t, c.reps[victim].db)
	if len(gotState) != len(refState) {
		t.Fatalf("restarted replica has %d keys, want %d", len(gotState), len(refState))
	}
	for k, v := range refState {
		if gotState[k] != v {
			t.Fatalf("restarted replica: %q = %q, want %q", k, gotState[k], v)
		}
	}
	for _, id := range c.ids {
		_ = c.reps[id].db.Close()
		_ = c.reps[id].st.Close()
	}
}

func TestDuplicateCommandAppliesExactlyOnce(t *testing.T) {
	c := newSMCluster(t, []uint64{1, 2, 3})
	lead := c.waitLeader(100)

	orig := &Command{ClientID: 7, Seq: 5, Batch: Batch{}}
	orig.Batch.Put([]byte("dup-key"), []byte("first"))
	if !c.propose(lead, orig) {
		t.Fatal("propose failed")
	}
	c.pump(20)

	// A malicious/retried duplicate with the same (client, seq) but a
	// different payload must be ignored by every replica.
	dup := &Command{ClientID: 7, Seq: 5, Batch: Batch{}}
	dup.Batch.Put([]byte("dup-key"), []byte("SECOND"))
	if !c.propose(lead, dup) {
		t.Fatal("propose dup failed")
	}
	// A later seq from the same client still applies.
	next := &Command{ClientID: 7, Seq: 6, Batch: Batch{}}
	next.Batch.Put([]byte("next-key"), []byte("yes"))
	if !c.propose(lead, next) {
		t.Fatal("propose next failed")
	}
	c.pump(30)

	for _, id := range c.ids {
		v, err := c.reps[id].db.Get([]byte("dup-key"))
		if err != nil || !bytes.Equal(v, []byte("first")) {
			t.Fatalf("replica %d: dup-key = %q, %v — duplicate was applied", id, v, err)
		}
		if v, err := c.reps[id].db.Get([]byte("next-key")); err != nil || !bytes.Equal(v, []byte("yes")) {
			t.Fatalf("replica %d: next-key = %q, %v", id, v, err)
		}
	}
	for _, id := range c.ids {
		_ = c.reps[id].db.Close()
		_ = c.reps[id].st.Close()
	}
}

func TestReservedRangeIsInvisibleAndProtected(t *testing.T) {
	db, err := Open(t.TempDir(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if err := db.Put(appliedKey, []byte("nope")); !errors.Is(err, ErrReservedKey) {
		t.Fatalf("user write to reserved key = %v", err)
	}
	if _, err := db.Get(appliedKey); !errors.Is(err, ErrReservedKey) {
		t.Fatalf("user read of reserved key = %v", err)
	}
	// Reserved keys written internally stay invisible to user scans.
	var b Batch
	b.Put([]byte("visible"), []byte("v"))
	if err := db.Apply(&b); err != nil {
		t.Fatal(err)
	}
	sm, err := NewStateMachine(db)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sm.Apply([]raft.Entry{{Index: 1, Term: 1, Data: nil}}); err != nil {
		t.Fatal(err)
	}
	keys, _ := collectScan(t, db.Scan(nil, nil))
	if len(keys) != 1 || keys[0] != "visible" {
		t.Fatalf("reserved keys leaked into user scan: %v", keys)
	}
}

func TestApplyNoDedupClient(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "db"), Options{DisableWAL: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	sm, err := NewStateMachine(db)
	if err != nil {
		t.Fatal(err)
	}

	idx := uint64(0)
	apply := func(cmd *Command) {
		t.Helper()
		idx++
		if _, err := sm.Apply([]raft.Entry{{Type: raft.EntryNormal, Index: idx, Term: 1, Data: EncodeCommand(cmd)}}); err != nil {
			t.Fatalf("apply: %v", err)
		}
	}
	put := func(k, v string) Batch {
		var b Batch
		b.Put([]byte(k), []byte(v))
		return b
	}
	get := func(k string) string {
		v, err := db.Get([]byte(k))
		if err != nil {
			t.Fatalf("get %q: %v", k, err)
		}
		return string(v)
	}

	// Ordinary client: a non-increasing seq is a duplicate and is skipped.
	apply(&Command{ClientID: 1, Seq: 1, Batch: put("a", "1")})
	apply(&Command{ClientID: 1, Seq: 1, Batch: put("a", "2")}) // duplicate: ignored
	if got := get("a"); got != "1" {
		t.Fatalf("ordinary duplicate applied: a=%q, want 1", got)
	}
	if sm.sessions[1] != 1 {
		t.Fatalf("ordinary client session = %d, want 1", sm.sessions[1])
	}

	// No-dedup client (high ClientID bit): the same seq applies every time and
	// records no session — so a client-id counter reset can never collide with
	// a persisted session, and ephemeral writers never leak sessions.
	nd := NoDedupClient | 7
	apply(&Command{ClientID: nd, Seq: 1, Batch: put("b", "1")})
	apply(&Command{ClientID: nd, Seq: 1, Batch: put("b", "2")}) // same seq, still applies
	if got := get("b"); got != "2" {
		t.Fatalf("no-dedup client did not re-apply: b=%q, want 2", got)
	}
	if _, ok := sm.sessions[nd]; ok {
		t.Fatalf("no-dedup client recorded a session: %d", sm.sessions[nd])
	}
}
