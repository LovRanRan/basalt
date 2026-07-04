package raft

import (
	"bytes"
	"testing"
)

func ent(index, term uint64, data string) Entry {
	return Entry{Index: index, Term: term, Data: []byte(data)}
}

func TestLogAppendAndBounds(t *testing.T) {
	l := newLog()
	if l.firstIndex() != 1 || l.lastIndex() != 0 || l.lastTerm() != 0 {
		t.Fatalf("empty log: first=%d last=%d term=%d", l.firstIndex(), l.lastIndex(), l.lastTerm())
	}
	l.append(ent(1, 1, "a"), ent(2, 1, "b"), ent(3, 2, "c"))
	if l.lastIndex() != 3 || l.lastTerm() != 2 {
		t.Fatalf("last=%d term=%d", l.lastIndex(), l.lastTerm())
	}
	if tm, ok := l.term(2); !ok || tm != 1 {
		t.Fatalf("term(2) = %d %v", tm, ok)
	}
	if _, ok := l.term(4); ok {
		t.Fatal("term past last must miss")
	}
	if tm, ok := l.term(0); !ok || tm != 0 {
		t.Fatalf("term(0) = %d %v", tm, ok)
	}
	got := l.slice(2, 4)
	if len(got) != 2 || string(got[0].Data) != "b" || string(got[1].Data) != "c" {
		t.Fatalf("slice = %v", got)
	}
	if l.slice(2, 2) != nil {
		t.Fatal("empty slice must be nil")
	}

	defer func() {
		if recover() == nil {
			t.Fatal("gapped append must panic")
		}
	}()
	l.append(ent(9, 2, "gap"))
}

func TestLogMaybeAppend(t *testing.T) {
	l := newLog()
	l.append(ent(1, 1, "a"), ent(2, 1, "b"), ent(3, 1, "c"))
	l.commitTo(1)

	// Missing prev entry rejects.
	if _, ok := l.maybeAppend(5, 1, 0, nil); ok {
		t.Fatal("missing prev must reject")
	}
	// Wrong prev term rejects.
	if _, ok := l.maybeAppend(3, 9, 0, nil); ok {
		t.Fatal("mismatched prev term must reject")
	}
	// Clean append past the end; the commit piggyback is capped by what
	// the leader says, not by what arrived.
	last, ok := l.maybeAppend(3, 1, 1, []Entry{ent(4, 2, "d")})
	if !ok || last != 4 || l.lastIndex() != 4 || l.committed != 1 {
		t.Fatalf("append: last=%d committed=%d", l.lastIndex(), l.committed)
	}
	// Duplicate prefix is idempotent; the UNCOMMITTED conflicting suffix
	// is truncated and replaced (committed entries can never conflict —
	// that is leader completeness, and truncating them panics).
	last, ok = l.maybeAppend(3, 1, 5, []Entry{ent(4, 3, "D"), ent(5, 3, "E")})
	if !ok || last != 5 || l.lastIndex() != 5 || l.committed != 5 {
		t.Fatalf("conflict append: last=%d committed=%d", l.lastIndex(), l.committed)
	}
	if tm, _ := l.term(4); tm != 3 {
		t.Fatalf("conflicting entry not replaced: term(4)=%d", tm)
	}
	// Entirely-contained duplicate leaves the log alone.
	if _, ok := l.maybeAppend(0, 0, 0, []Entry{ent(1, 1, "a")}); !ok || l.lastIndex() != 5 {
		t.Fatalf("duplicate prefix mutated the log: last=%d", l.lastIndex())
	}

	defer func() {
		if recover() == nil {
			t.Fatal("truncating committed entries must panic")
		}
	}()
	l.truncateFrom(2)
}

func TestLogIsUpToDate(t *testing.T) {
	l := newLog()
	l.append(ent(1, 1, ""), ent(2, 3, ""))
	cases := []struct {
		idx, term uint64
		want      bool
	}{
		{2, 3, true},  // identical
		{3, 3, true},  // longer same term
		{1, 4, true},  // higher term wins regardless of length
		{9, 2, false}, // lower term loses regardless of length
		{1, 3, false}, // same term, shorter
	}
	for _, c := range cases {
		if got := l.isUpToDate(c.idx, c.term); got != c.want {
			t.Errorf("isUpToDate(%d,%d) = %v, want %v", c.idx, c.term, got, c.want)
		}
	}
}

func TestTermRules(t *testing.T) {
	n := NewNode(1, []uint64{1, 2, 3})
	n.becomeCandidate() // term 1, voted self
	if n.Term() != 1 || n.vote != 1 {
		t.Fatalf("candidate: term=%d vote=%d", n.Term(), n.vote)
	}
	// A higher term demotes and clears the vote.
	if err := n.Step(Message{Type: MsgApp, From: 2, Term: 5}); err != nil {
		t.Fatal(err)
	}
	if n.Role() != Follower || n.Term() != 5 || n.vote != 0 || n.Lead() != 2 {
		t.Fatalf("after higher-term MsgApp: %v term=%d vote=%d lead=%d", n.Role(), n.Term(), n.vote, n.Lead())
	}
	// Same-term reset must NOT clear the vote.
	n.vote = 2
	n.becomeFollower(5, 0)
	if n.vote != 2 {
		t.Fatal("same-term transition cleared the vote")
	}
	// Stale messages are ignored.
	if err := n.Step(Message{Type: MsgApp, From: 3, Term: 1}); err != nil {
		t.Fatal(err)
	}
	if n.Term() != 5 {
		t.Fatalf("stale message moved the term to %d", n.Term())
	}
}

func TestRoleTransitionInvariants(t *testing.T) {
	n := NewNode(1, []uint64{1})
	n.Campaign()
	if n.Role() != Leader {
		t.Fatalf("single node must self-elect, got %v", n.Role())
	}
	defer func() {
		if recover() == nil {
			t.Fatal("leader -> candidate must panic")
		}
	}()
	n.becomeCandidate()
}

func TestProposeToNonLeader(t *testing.T) {
	n := NewNode(1, []uint64{1, 2, 3})
	if err := n.Propose([]byte("x")); err != ErrNotLeader {
		t.Fatalf("err = %v, want ErrNotLeader", err)
	}
}

// drain runs the Ready loop to quiescence, returning every applied entry
// and outbound message. Zero goroutines, timers, or sockets.
func drain(t *testing.T, n *Node) (applied []Entry, sent []Message) {
	t.Helper()
	for i := 0; n.HasReady(); i++ {
		if i > 100 {
			t.Fatal("ready loop does not quiesce")
		}
		rd := n.Ready()
		if rd.HardState != nil && rd.HardState.Term < n.prevHard.Term {
			t.Fatal("hard state term regressed")
		}
		applied = append(applied, rd.CommittedEntries...)
		sent = append(sent, rd.Messages...)
		n.Advance(rd)
	}
	return applied, sent
}

func TestSingleNodeLifecycle(t *testing.T) {
	n := NewNode(1, []uint64{1})
	n.Campaign()
	if n.Role() != Leader || n.Term() != 1 {
		t.Fatalf("role=%v term=%d", n.Role(), n.Term())
	}

	applied, sent := drain(t, n)
	if len(sent) != 0 {
		t.Fatalf("single node sent %d messages", len(sent))
	}
	if len(applied) != 1 || applied[0].Data != nil {
		t.Fatalf("expected the term-opening no-op to commit, got %v", applied)
	}

	if err := n.Propose([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	if err := n.Propose([]byte("world")); err != nil {
		t.Fatal(err)
	}
	applied, _ = drain(t, n)
	if len(applied) != 2 || !bytes.Equal(applied[0].Data, []byte("hello")) || !bytes.Equal(applied[1].Data, []byte("world")) {
		t.Fatalf("applied = %v", applied)
	}
	if n.Committed() != 3 {
		t.Fatalf("committed = %d, want 3", n.Committed())
	}

	// Commit never outruns persistence: before Advance, committed entries
	// stop at the stabled prefix.
	if err := n.Propose([]byte("tail")); err != nil {
		t.Fatal(err)
	}
	rd := n.Ready()
	for _, e := range rd.CommittedEntries {
		if bytes.Equal(e.Data, []byte("tail")) {
			t.Fatal("entry applied before it was persisted")
		}
	}
	n.Advance(rd)
	applied, _ = drain(t, n)
	if len(applied) != 1 || !bytes.Equal(applied[0].Data, []byte("tail")) {
		t.Fatalf("tail apply = %v", applied)
	}
}

func TestCampaignEmitsVoteRequests(t *testing.T) {
	n := NewNode(1, []uint64{1, 2, 3})
	n.Campaign()
	if n.Role() != Candidate {
		t.Fatalf("role = %v, want candidate", n.Role())
	}
	_, sent := drain(t, n)
	votes := 0
	seen := map[uint64]bool{}
	for _, m := range sent {
		if m.Type != MsgVote {
			t.Fatalf("unexpected message %v", m)
		}
		if m.Term != 1 || m.From != 1 {
			t.Fatalf("vote request %+v", m)
		}
		seen[m.To] = true
		votes++
	}
	if votes != 2 || !seen[2] || !seen[3] {
		t.Fatalf("vote requests to %v", seen)
	}
	// The bumped term with our self-vote must have been surfaced for
	// persistence before those messages were handed out.
	if n.prevHard.Term != 1 || n.prevHard.Vote != 1 {
		t.Fatalf("hard state not surfaced: %+v", n.prevHard)
	}
}
