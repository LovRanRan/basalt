package raft

import (
	"os"
	"path/filepath"
	"testing"
)

func reopen(t *testing.T, path string) (HardState, []Entry) {
	t.Helper()
	s, hs, ents, err := OpenStorage(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	return hs, ents
}

func TestStorageRoundtrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft", "state")
	s, hs, ents, err := OpenStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	if hs != (HardState{}) || len(ents) != 0 {
		t.Fatalf("fresh storage: hs=%+v ents=%d", hs, len(ents))
	}
	if err := s.SaveHardState(HardState{Term: 3, Vote: 2, Commit: 0}); err != nil {
		t.Fatal(err)
	}
	if err := s.AppendEntries([]Entry{ent(1, 1, "a"), ent(2, 3, "b")}); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveHardState(HardState{Term: 3, Vote: 2, Commit: 2}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	hs, ents = reopen(t, path)
	if hs.Term != 3 || hs.Vote != 2 || hs.Commit != 2 {
		t.Fatalf("recovered hs = %+v", hs)
	}
	if len(ents) != 2 || string(ents[1].Data) != "b" || ents[1].Term != 3 {
		t.Fatalf("recovered ents = %v", ents)
	}
}

func TestStorageTruncateSuffix(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state")
	s, _, _, err := OpenStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.AppendEntries([]Entry{ent(1, 1, "a"), ent(2, 1, "b"), ent(3, 1, "c")}); err != nil {
		t.Fatal(err)
	}
	// A conflicting suffix at index 2 replaces b,c with B,D.
	if err := s.AppendEntries([]Entry{ent(2, 2, "B"), ent(3, 2, "D")}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	_, ents := reopen(t, path)
	if len(ents) != 3 {
		t.Fatalf("recovered %d entries, want 3", len(ents))
	}
	if string(ents[0].Data) != "a" || string(ents[1].Data) != "B" || string(ents[2].Data) != "D" {
		t.Fatalf("truncation not recovered: %v", ents)
	}
	if ents[1].Term != 2 || ents[2].Term != 2 {
		t.Fatalf("replaced entries kept old term: %v", ents)
	}
}

func TestStorageTornTailRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state")
	s, _, _, err := OpenStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SaveHardState(HardState{Term: 5, Vote: 1}); err != nil {
		t.Fatal(err)
	}
	if err := s.AppendEntries([]Entry{ent(1, 5, "committed")}); err != nil {
		t.Fatal(err)
	}
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Simulate a crash mid-append: garbage after the committed prefix.
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{0xde, 0xad, 0xbe, 0xef}); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	hs, ents := reopen(t, path)
	if hs.Term != 5 || hs.Vote != 1 {
		t.Fatalf("torn tail lost hard state: %+v", hs)
	}
	if len(ents) != 1 || string(ents[0].Data) != "committed" {
		t.Fatalf("torn tail lost the committed entry: %v", ents)
	}
}

func TestRestoreNodeResumesWithoutRevoting(t *testing.T) {
	// A node that voted in term 5 must, after restart, still refuse a
	// second vote in term 5.
	cfg := Config{ID: 1, Peers: []uint64{1, 2, 3}}
	n := RestoreNode(cfg, HardState{Term: 5, Vote: 2, Commit: 0}, []Entry{ent(1, 5, "x")}, 0)
	if n.Term() != 5 || n.Role() != Follower {
		t.Fatalf("restored role=%v term=%d", n.Role(), n.Term())
	}
	// A candidate at the same term that we already voted against gets a
	// rejection.
	n.handleVote(Message{Type: MsgVote, From: 3, Term: 5, LogIndex: 1, LogTerm: 5})
	rd := n.Ready()
	found := false
	for _, m := range rd.Messages {
		if m.Type == MsgVoteResp {
			found = true
			if !m.Reject {
				t.Fatal("restored node granted a second vote in the same term")
			}
		}
	}
	if !found {
		t.Fatal("no vote response emitted")
	}
	// Restored entries are already persisted: Ready must not re-offer them.
	if len(rd.Entries) != 0 {
		t.Fatalf("restored node re-emitted persisted entries: %v", rd.Entries)
	}
}

func TestPersistThenRestoreDrivesReadyLoop(t *testing.T) {
	// End-to-end: run a single-node leader through SaveReady, reopen the
	// storage, and confirm the committed commands survived.
	dir := t.TempDir()
	path := filepath.Join(dir, "state")
	st, hs, ents, err := OpenStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	n := RestoreNode(Config{ID: 1, Peers: []uint64{1}}, hs, ents, 0)
	n.Campaign()
	if err := n.Propose([]byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := n.Propose([]byte("two")); err != nil {
		t.Fatal(err)
	}
	for n.HasReady() {
		rd := n.Ready()
		if err := st.SaveReady(rd); err != nil {
			t.Fatal(err)
		}
		n.Advance(rd)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	_, ents2 := reopen(t, path)
	var cmds []string
	for _, e := range ents2 {
		if e.Data != nil {
			cmds = append(cmds, string(e.Data))
		}
	}
	if len(cmds) != 2 || cmds[0] != "one" || cmds[1] != "two" {
		t.Fatalf("persisted commands = %v", cmds)
	}
}
