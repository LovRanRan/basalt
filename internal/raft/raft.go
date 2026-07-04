// Package raft implements the Raft consensus core as a deterministic,
// transport-free state machine: callers feed it messages via Step and time
// via Tick, and drain the effects — messages to send, entries and hard
// state to persist, entries to apply — through Ready/Advance. All I/O
// (clock, network, disk) lives outside this package, which is what makes
// every test of it deterministic.
package raft

import (
	"errors"
	"fmt"
	"sort"
)

type Role uint8

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	default:
		return "leader"
	}
}

type MessageType uint8

const (
	// MsgProp is a local proposal; Entries carry opaque commands.
	MsgProp MessageType = iota
	// MsgVote is RequestVote: LogIndex/LogTerm describe the candidate's
	// last entry.
	MsgVote
	MsgVoteResp
	// MsgApp is AppendEntries: LogIndex/LogTerm are the consistency-check
	// previous entry, Commit piggybacks the leader's commit index. Empty
	// Entries is the heartbeat.
	MsgApp
	// MsgAppResp reports LogIndex = highest matching index on success, or
	// Reject with LogIndex hinting where to back off.
	MsgAppResp
)

type Entry struct {
	Index uint64
	Term  uint64
	Data  []byte
}

// HardState is the durable part of a node: it must be fsynced before any
// message that depends on it leaves the process.
type HardState struct {
	Term   uint64
	Vote   uint64
	Commit uint64
}

type Message struct {
	Type     MessageType
	From, To uint64
	Term     uint64
	LogIndex uint64
	LogTerm  uint64
	Entries  []Entry
	Commit   uint64
	Reject   bool
}

// Ready is one batch of effects. The caller must persist HardState (when
// non-nil) and Entries durably BEFORE sending Messages, then apply
// CommittedEntries, then call Advance.
type Ready struct {
	HardState        *HardState
	Entries          []Entry
	CommittedEntries []Entry
	Messages         []Message
}

// ErrNotLeader rejects proposals stepped into a non-leader.
var ErrNotLeader = errors.New("raft: not the leader")

// Node is the consensus state machine for one member. Not safe for
// concurrent use: the owner serializes Step/Tick/Ready/Advance.
type Node struct {
	id    uint64
	peers []uint64

	role Role
	term uint64
	vote uint64
	lead uint64

	log   *raftLog
	votes map[uint64]bool

	// Leader replication state (Figure 2's nextIndex/matchIndex).
	next  map[uint64]uint64
	match map[uint64]uint64

	msgs []Message

	// stabled is the highest log index the caller has persisted; only
	// persisted entries may be applied or counted as our own match.
	stabled  uint64
	prevHard HardState

	electionElapsed int
}

// NewNode creates a follower at term 0. peers lists every member id,
// including the node's own.
func NewNode(id uint64, peers []uint64) *Node {
	found := false
	for _, p := range peers {
		if p == id {
			found = true
		}
	}
	if !found {
		panic(fmt.Sprintf("raft: node %d missing from peers %v", id, peers))
	}
	return &Node{
		id:    id,
		peers: append([]uint64(nil), peers...),
		log:   newLog(),
		votes: map[uint64]bool{},
	}
}

func (n *Node) quorum() int { return len(n.peers)/2 + 1 }

// reset moves to a new term: the vote resets only when the term actually
// changes — re-voting within one term is the one thing Raft must never do.
func (n *Node) reset(term uint64) {
	if term < n.term {
		panic(fmt.Sprintf("raft: term regression %d -> %d", n.term, term))
	}
	if term > n.term {
		n.term = term
		n.vote = 0
	}
	n.lead = 0
	n.votes = map[uint64]bool{}
	n.electionElapsed = 0
}

func (n *Node) becomeFollower(term, lead uint64) {
	n.reset(term)
	n.role = Follower
	n.lead = lead
}

func (n *Node) becomeCandidate() {
	if n.role == Leader {
		panic("raft: leader cannot become candidate directly")
	}
	n.reset(n.term + 1)
	n.role = Candidate
	n.vote = n.id
	n.votes[n.id] = true
}

func (n *Node) becomeLeader() {
	if n.role != Candidate {
		panic("raft: only a candidate can become leader")
	}
	n.role = Leader
	n.lead = n.id
	n.next = map[uint64]uint64{}
	n.match = map[uint64]uint64{}
	for _, p := range n.peers {
		n.next[p] = n.log.lastIndex() + 1
		n.match[p] = 0
	}
	// The no-op entry: committing something from the new term is the only
	// way earlier-term entries may commit (the Figure 8 rule).
	n.log.append(Entry{Index: n.log.lastIndex() + 1, Term: n.term})
	n.maybeCommit()
}

// Campaign starts an election. Timer-driven campaigns arrive with P3.2;
// tests and single-node bootstrap call this directly.
func (n *Node) Campaign() {
	n.becomeCandidate()
	if len(n.votes) >= n.quorum() {
		n.becomeLeader()
		return
	}
	for _, p := range n.peers {
		if p == n.id {
			continue
		}
		n.send(Message{
			Type:     MsgVote,
			To:       p,
			LogIndex: n.log.lastIndex(),
			LogTerm:  n.log.lastTerm(),
		})
	}
}

func (n *Node) send(m Message) {
	m.From = n.id
	if m.Term == 0 {
		m.Term = n.term
	}
	n.msgs = append(n.msgs, m)
}

// Tick advances logical time by one unit. Election and heartbeat timeouts
// land in P3.2; the counter exists so the Ready loop shape is final.
func (n *Node) Tick() {
	n.electionElapsed++
}

// Step feeds one message into the state machine.
func (n *Node) Step(m Message) error {
	switch {
	case m.Type == MsgProp:
		return n.handlePropose(m)
	case m.Term > n.term:
		// Any newer term makes us a follower of that term; only an
		// actual AppendEntries identifies the leader.
		lead := uint64(0)
		if m.Type == MsgApp {
			lead = m.From
		}
		n.becomeFollower(m.Term, lead)
	case m.Term < n.term:
		// Stale sender: full responses land with the vote (P3.2) and
		// replication (P3.4) handlers; dropping is always safe.
		return nil
	}
	// Same-term dispatch arrives with P3.2 (votes) and P3.4 (append).
	return nil
}

func (n *Node) handlePropose(m Message) error {
	if n.role != Leader {
		return ErrNotLeader
	}
	li := n.log.lastIndex()
	ents := make([]Entry, len(m.Entries))
	for i, e := range m.Entries {
		ents[i] = Entry{Index: li + 1 + uint64(i), Term: n.term, Data: e.Data}
	}
	n.log.append(ents...)
	n.maybeCommit()
	return nil
}

// Propose submits one command.
func (n *Node) Propose(data []byte) error {
	return n.Step(Message{Type: MsgProp, Entries: []Entry{{Data: data}}})
}

// maybeCommit advances the commit index to the highest entry from the
// current term that a quorum has matched. The leader's own match is its
// persisted (stabled) prefix, not its in-memory tail.
func (n *Node) maybeCommit() bool {
	if n.role != Leader {
		return false
	}
	n.match[n.id] = min(n.stabled, n.log.lastIndex())
	matches := make([]uint64, 0, len(n.peers))
	for _, p := range n.peers {
		matches = append(matches, n.match[p])
	}
	sort.Slice(matches, func(i, j int) bool { return matches[i] > matches[j] })
	candidate := matches[n.quorum()-1]
	if candidate <= n.log.committed {
		return false
	}
	if t, ok := n.log.term(candidate); !ok || t != n.term {
		return false
	}
	n.log.commitTo(candidate)
	return true
}

func (n *Node) hardState() HardState {
	return HardState{Term: n.term, Vote: n.vote, Commit: n.log.committed}
}

// HasReady reports whether Ready would carry anything.
func (n *Node) HasReady() bool {
	if len(n.msgs) > 0 || n.hardState() != n.prevHard {
		return true
	}
	if n.log.lastIndex() > n.stabled {
		return true
	}
	return min(n.log.committed, n.stabled) > n.log.applied
}

// Ready drains the pending effects. Callers must Advance with the same
// Ready once they have persisted, sent, and applied it.
func (n *Node) Ready() Ready {
	rd := Ready{Messages: n.msgs}
	n.msgs = nil
	if hs := n.hardState(); hs != n.prevHard {
		rd.HardState = &hs
	}
	if n.log.lastIndex() > n.stabled {
		rd.Entries = append([]Entry(nil), n.log.slice(n.stabled+1, n.log.lastIndex()+1)...)
	}
	// Only persisted entries are applied: a crash after apply but before
	// persist would otherwise replay differently.
	if applyTo := min(n.log.committed, n.stabled); applyTo > n.log.applied {
		rd.CommittedEntries = append([]Entry(nil), n.log.slice(n.log.applied+1, applyTo+1)...)
	}
	return rd
}

// Advance acknowledges a Ready: its entries are durable and its committed
// entries are applied.
func (n *Node) Advance(rd Ready) {
	if rd.HardState != nil {
		n.prevHard = *rd.HardState
	}
	if len(rd.Entries) > 0 {
		n.stabled = rd.Entries[len(rd.Entries)-1].Index
		n.maybeCommit() // our own persistence may complete a quorum
	}
	if len(rd.CommittedEntries) > 0 {
		n.log.appliedTo(rd.CommittedEntries[len(rd.CommittedEntries)-1].Index)
	}
}

// Accessors for tests and upper layers.
func (n *Node) ID() uint64        { return n.id }
func (n *Node) Term() uint64      { return n.term }
func (n *Node) Role() Role        { return n.role }
func (n *Node) Lead() uint64      { return n.lead }
func (n *Node) Committed() uint64 { return n.log.committed }
