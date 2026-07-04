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
	// MsgPreVote / MsgPreVoteResp are the PreVote round: a probe that does
	// NOT bump the term, so a partitioned node cannot depose a healthy
	// leader on heal. Campaign is a real election only after PreVote wins.
	MsgPreVote
	MsgPreVoteResp
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

// Config parameters a Node. ElectionTick is the baseline election timeout
// in Tick units; the actual timeout is randomized in [ElectionTick,
// 2*ElectionTick) from Rand so split votes resolve. HeartbeatTick is the
// leader's heartbeat period. Rand returns a non-negative int below its
// argument (an injected math/rand for determinism); nil uses a fixed
// sequence so a Node without a Config is still deterministic.
type Config struct {
	ID            uint64
	Peers         []uint64
	ElectionTick  int
	HeartbeatTick int
	Rand          func(n int) int
}

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

	// preVotes tallies a PreVote round separately from real votes.
	preVotes map[uint64]bool

	// Leader replication state (Figure 2's nextIndex/matchIndex).
	next  map[uint64]uint64
	match map[uint64]uint64

	msgs []Message

	// stabled is the highest log index the caller has persisted; only
	// persisted entries may be applied or counted as our own match.
	stabled  uint64
	prevHard HardState

	electionTick    int
	heartbeatTick   int
	rand            func(n int) int
	randSeq         uint64
	electionElapsed int
	heartbeatElapse int
	randomTimeout   int
}

// NewNode creates a follower at term 0 with default timing (10-tick
// elections, 1-tick heartbeat). peers lists every member id including this
// one.
func NewNode(id uint64, peers []uint64) *Node {
	return NewNodeConfig(Config{ID: id, Peers: peers})
}

func NewNodeConfig(cfg Config) *Node {
	return newNode(cfg, Recovered{}, 0)
}

// Recovered is the durable state read back from Storage: the hard state,
// the snapshot boundary the log was compacted to, and the surviving
// entries (contiguous from SnapIndex+1).
type Recovered struct {
	HardState HardState
	SnapIndex uint64
	SnapTerm  uint64
	Entries   []Entry
}

// RestoreNode rebuilds a node from persisted state. It resumes as a
// follower at the persisted term — never re-voting, never regressing — and
// re-emits committed-but-unapplied entries so the state machine catches up.
//
// appliedIndex is the index the state machine has durably applied through;
// it must be at least rec.SnapIndex (entries below the snapshot boundary
// are gone — compaction never passes the durable applied index, so a
// smaller value here means the compaction rule was violated). Entries in
// (appliedIndex, commit] are re-offered as CommittedEntries after restore.
func RestoreNode(cfg Config, rec Recovered, appliedIndex uint64) *Node {
	return newNode(cfg, rec, appliedIndex)
}

func newNode(cfg Config, rec Recovered, appliedIndex uint64) *Node {
	hs, ents := rec.HardState, rec.Entries
	found := false
	for _, p := range cfg.Peers {
		if p == cfg.ID {
			found = true
		}
	}
	if !found {
		panic(fmt.Sprintf("raft: node %d missing from peers %v", cfg.ID, cfg.Peers))
	}
	if cfg.ElectionTick <= 0 {
		cfg.ElectionTick = 10
	}
	if cfg.HeartbeatTick <= 0 {
		cfg.HeartbeatTick = 1
	}
	n := &Node{
		id:            cfg.ID,
		peers:         append([]uint64(nil), cfg.Peers...),
		log:           newLog(),
		votes:         map[uint64]bool{},
		preVotes:      map[uint64]bool{},
		electionTick:  cfg.ElectionTick,
		heartbeatTick: cfg.HeartbeatTick,
		rand:          cfg.Rand,
	}
	n.term = hs.Term
	n.vote = hs.Vote
	if rec.SnapIndex > 0 {
		n.log.offset = rec.SnapIndex + 1
		n.log.snapIndex, n.log.snapTerm = rec.SnapIndex, rec.SnapTerm
		n.log.committed = rec.SnapIndex
		n.log.applied = rec.SnapIndex
	}
	if len(ents) > 0 {
		n.log.append(ents...)
	}
	if hs.Commit > 0 {
		n.log.commitTo(hs.Commit)
	}
	// The recovered log is already durable; the state machine has applied
	// through appliedIndex, so entries in (appliedIndex, commit] are
	// re-offered on the first Ready.
	n.stabled = n.log.lastIndex()
	if appliedIndex > n.log.committed {
		panic(fmt.Sprintf("raft: applied index %d beyond commit %d", appliedIndex, n.log.committed))
	}
	if appliedIndex < rec.SnapIndex {
		panic(fmt.Sprintf("raft: applied index %d below snapshot %d — compaction outran the durable state machine", appliedIndex, rec.SnapIndex))
	}
	n.log.applied = appliedIndex
	n.prevHard = n.hardState()
	n.resetElectionTimer()
	return n
}

// intn returns a deterministic non-negative int below hi; without an
// injected Rand it walks a fixed splitmix sequence.
func (n *Node) intn(hi int) int {
	if hi <= 0 {
		return 0
	}
	if n.rand != nil {
		return n.rand(hi)
	}
	n.randSeq += 0x9e3779b97f4a7c15
	z := n.randSeq
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	z ^= z >> 31
	return int(z % uint64(hi))
}

func (n *Node) resetElectionTimer() {
	n.electionElapsed = 0
	n.randomTimeout = n.electionTick + n.intn(n.electionTick)
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
	n.preVotes = map[uint64]bool{}
	n.resetElectionTimer()
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

// becomePreCandidate begins a PreVote round WITHOUT bumping the term.
func (n *Node) becomePreCandidate() {
	if n.role == Leader {
		panic("raft: leader cannot pre-campaign")
	}
	n.role = Candidate
	n.lead = 0
	n.preVotes = map[uint64]bool{n.id: true}
	n.resetElectionTimer()
}

func (n *Node) becomeLeader() {
	if n.role != Candidate {
		panic("raft: only a candidate can become leader")
	}
	n.role = Leader
	n.lead = n.id
	n.heartbeatElapse = 0
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
	// Assert authority immediately so followers learn the leader without
	// waiting a heartbeat tick.
	n.bcastHeartbeat()
}

// Campaign starts a PreVote round; winning it promotes to a real election.
// Tests and single-node bootstrap call this directly; the election timeout
// calls it from Tick.
func (n *Node) Campaign() {
	n.becomePreCandidate()
	if len(n.preVotes) >= n.quorum() {
		n.startElection()
		return
	}
	// PreVote asks at term+1 without adopting it.
	n.bcastVote(MsgPreVote, n.term+1)
}

// startElection is the real election after PreVote succeeds.
func (n *Node) startElection() {
	n.becomeCandidate()
	if len(n.votes) >= n.quorum() {
		n.becomeLeader()
		return
	}
	n.bcastVote(MsgVote, n.term)
}

func (n *Node) bcastVote(t MessageType, term uint64) {
	for _, p := range n.peers {
		if p == n.id {
			continue
		}
		n.send(Message{
			Type:     t,
			To:       p,
			Term:     term,
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

// Tick advances logical time. A follower or candidate that reaches its
// randomized election timeout starts a campaign; a leader beats its
// heartbeat (heartbeat content lands with replication in P3.4).
func (n *Node) Tick() {
	if n.role == Leader {
		n.heartbeatElapse++
		if n.heartbeatElapse >= n.heartbeatTick {
			n.heartbeatElapse = 0
			n.bcastHeartbeat()
		}
		return
	}
	n.electionElapsed++
	if n.electionElapsed >= n.randomTimeout {
		n.Campaign()
	}
}

// bcastAppend sends each follower the entries after its nextIndex.
func (n *Node) bcastAppend() {
	for _, p := range n.peers {
		if p != n.id {
			n.sendAppend(p)
		}
	}
}

// bcastHeartbeat is bcastAppend by another name — a heartbeat is just an
// AppendEntries that usually carries no new entries.
func (n *Node) bcastHeartbeat() { n.bcastAppend() }

// sendAppend ships AppendEntries to one follower: the consistency-check
// previous entry at nextIndex-1, then every entry from nextIndex onward,
// with the commit index piggybacked (capped at what the follower is known
// to have, so it never commits an entry it lacks).
func (n *Node) sendAppend(to uint64) {
	next := n.next[to]
	prevIndex := next - 1
	prevTerm, ok := n.log.term(prevIndex)
	if !ok {
		// The follower is behind the log's first index; InstallSnapshot
		// (P3.8) handles this. Until then, retreat to what we have.
		prevIndex = n.log.firstIndex() - 1
		prevTerm, _ = n.log.term(prevIndex)
		next = prevIndex + 1
	}
	ents := append([]Entry(nil), n.log.slice(next, n.log.lastIndex()+1)...)
	n.send(Message{
		Type:     MsgApp,
		To:       to,
		LogIndex: prevIndex,
		LogTerm:  prevTerm,
		Entries:  ents,
		Commit:   min(n.log.committed, prevIndex+uint64(len(ents))),
	})
}

// Step feeds one message into the state machine.
func (n *Node) Step(m Message) error {
	if m.Type == MsgProp {
		return n.handlePropose(m)
	}
	// A PreVote/PreVoteResp carries a probe term it has not adopted, so it
	// must never drive our own term up or down; handle it before the
	// term-comparison rules.
	if m.Type == MsgPreVote {
		n.handlePreVote(m)
		return nil
	}
	if m.Type == MsgPreVoteResp {
		n.handlePreVoteResp(m)
		return nil
	}

	switch {
	case m.Term > n.term:
		lead := uint64(0)
		if m.Type == MsgApp {
			lead = m.From
		}
		n.becomeFollower(m.Term, lead)
	case m.Term < n.term:
		// Stale sender. A stale MsgApp/MsgVote gets a rejection carrying
		// our real term so the sender steps down; other stale messages
		// are safe to drop.
		if m.Type == MsgVote {
			n.send(Message{Type: MsgVoteResp, To: m.From, Reject: true})
		}
		return nil
	}

	switch m.Type {
	case MsgVote:
		n.handleVote(m)
	case MsgVoteResp:
		n.handleVoteResp(m)
	case MsgApp:
		n.handleApp(m)
	case MsgAppResp:
		n.handleAppResp(m)
	}
	return nil
}

// handleApp processes AppendEntries: adopt the sender as leader, run the
// log consistency check, append (truncating conflicts), advance commit, and
// reply with the new match index — or reject with a conflict hint for fast
// backup.
func (n *Node) handleApp(m Message) {
	n.role = Follower
	n.lead = m.From
	n.resetElectionTimer()

	lastNew, ok := n.log.maybeAppend(m.LogIndex, m.LogTerm, m.Commit, m.Entries)
	if !ok {
		// Reject with a hint so the leader backs up by terms, not one
		// index at a time: point it at the first index we could match.
		hint := n.log.lastIndex() + 1
		if m.LogIndex < hint {
			hint = m.LogIndex
		}
		n.send(Message{Type: MsgAppResp, To: m.From, Reject: true, LogIndex: hint})
		return
	}
	n.send(Message{Type: MsgAppResp, To: m.From, LogIndex: lastNew})
}

// handleAppResp advances a follower's match/next on success (and re-sends if
// more remains), or backs its nextIndex up to the hint on rejection.
func (n *Node) handleAppResp(m Message) {
	if n.role != Leader {
		return
	}
	if m.Reject {
		if m.LogIndex < n.next[m.From] {
			n.next[m.From] = max(m.LogIndex, 1)
		}
		n.sendAppend(m.From)
		return
	}
	if m.LogIndex > n.match[m.From] {
		n.match[m.From] = m.LogIndex
		n.next[m.From] = m.LogIndex + 1
		if n.maybeCommit() {
			// A new commit index must reach followers promptly.
			n.bcastAppend()
		}
	}
}

// handlePreVote grants a pre-vote when the probe term is ahead of ours and
// the candidate's log is at least as up to date. Granting does not change
// our term or vote — it only says "an election at that term could win".
func (n *Node) handlePreVote(m Message) {
	grant := m.Term > n.term && n.log.isUpToDate(m.LogIndex, m.LogTerm)
	n.send(Message{Type: MsgPreVoteResp, To: m.From, Term: m.Term, Reject: !grant})
}

func (n *Node) handlePreVoteResp(m Message) {
	if n.role != Candidate || len(n.votes) > 0 {
		return // already promoted to a real election, or not campaigning
	}
	if !m.Reject {
		n.preVotes[m.From] = true
	}
	if len(n.preVotes) >= n.quorum() {
		n.startElection()
	}
}

// handleVote grants at most one real vote per term, to an up-to-date
// candidate.
func (n *Node) handleVote(m Message) {
	grant := (n.vote == 0 || n.vote == m.From) && n.log.isUpToDate(m.LogIndex, m.LogTerm)
	if grant {
		n.vote = m.From
		n.resetElectionTimer()
	}
	n.send(Message{Type: MsgVoteResp, To: m.From, Reject: !grant})
}

func (n *Node) handleVoteResp(m Message) {
	if n.role != Candidate {
		return
	}
	if !m.Reject {
		n.votes[m.From] = true
	}
	if len(n.votes) >= n.quorum() {
		n.becomeLeader()
	}
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
	n.bcastAppend() // replicate promptly, not on the next heartbeat
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

// CompactTo folds the log prefix through index into a snapshot boundary.
// The caller has already captured the state machine at that index (an
// engine checkpoint) and must persist the compaction (Storage.Rewrite)
// before or with its next sync. index must not pass the applied index.
func (n *Node) CompactTo(index uint64) {
	n.log.compactTo(index)
}

// SnapIndex and SnapTerm describe the log's compacted boundary.
func (n *Node) SnapIndex() uint64 { return n.log.snapIndex }
func (n *Node) SnapTerm() uint64  { return n.log.snapTerm }

// Entries returns the live log suffix (everything after the snapshot
// boundary); Storage.Rewrite persists exactly this alongside the boundary.
func (n *Node) Entries() []Entry {
	return append([]Entry(nil), n.log.slice(n.log.firstIndex(), n.log.lastIndex()+1)...)
}

// HardStateNow returns the current durable triple, for Storage.Rewrite.
func (n *Node) HardStateNow() HardState { return n.hardState() }

// Accessors for tests and upper layers.
func (n *Node) ID() uint64        { return n.id }
func (n *Node) Term() uint64      { return n.term }
func (n *Node) Role() Role        { return n.role }
func (n *Node) Lead() uint64      { return n.lead }
func (n *Node) Committed() uint64 { return n.log.committed }
