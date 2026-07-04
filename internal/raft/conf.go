package raft

import (
	"encoding/binary"
	"fmt"
)

// ConfChangeType is the action a configuration change performs.
type ConfChangeType uint8

const (
	// ConfAddNode adds a voting member (or promotes a learner to voter).
	ConfAddNode ConfChangeType = iota
	// ConfRemoveNode removes a member entirely.
	ConfRemoveNode
	// ConfAddLearner adds a non-voting member that receives the log but
	// does not count toward quorum until promoted with ConfAddNode.
	ConfAddLearner
)

// ConfChange describes one single-server membership change.
type ConfChange struct {
	Type ConfChangeType
	Node uint64
}

// Encode serializes a ConfChange for an EntryConfChange entry.
func (cc ConfChange) Encode() []byte {
	buf := make([]byte, 9)
	buf[0] = byte(cc.Type)
	binary.LittleEndian.PutUint64(buf[1:], cc.Node)
	return buf
}

func decodeConfChange(data []byte) (ConfChange, error) {
	if len(data) != 9 {
		return ConfChange{}, fmt.Errorf("raft: bad conf change (%d bytes)", len(data))
	}
	return ConfChange{Type: ConfChangeType(data[0]), Node: binary.LittleEndian.Uint64(data[1:])}, nil
}

// applyConfChange mutates the voter/learner sets. It is idempotent: adding
// an existing voter or removing an absent node is a no-op.
func (n *Node) applyConfChange(cc ConfChange) {
	switch cc.Type {
	case ConfAddNode:
		delete(n.learners, cc.Node)
		n.voters[cc.Node] = true
		n.initProgress(cc.Node)
	case ConfAddLearner:
		if !n.voters[cc.Node] {
			n.learners[cc.Node] = true
			n.initProgress(cc.Node)
		}
	case ConfRemoveNode:
		delete(n.voters, cc.Node)
		delete(n.learners, cc.Node)
		delete(n.next, cc.Node)
		delete(n.match, cc.Node)
		if n.transferee == cc.Node {
			n.transferee = 0
		}
	}
	n.rebuildPeers()
}

// initProgress seeds a newly-added member's replication state when this node
// leads, so the leader immediately starts shipping it the log.
func (n *Node) initProgress(id uint64) {
	if n.role != Leader || id == n.id {
		return
	}
	if n.next == nil {
		n.next = map[uint64]uint64{}
	}
	if n.match == nil {
		n.match = map[uint64]uint64{}
	}
	if _, ok := n.next[id]; !ok {
		n.next[id] = n.log.lastIndex() + 1
		n.match[id] = 0
	}
}

// rebuildPeers recomputes the message-target list (voters + learners).
func (n *Node) rebuildPeers() {
	n.peers = n.peers[:0]
	for id := range n.voters {
		n.peers = append(n.peers, id)
	}
	for id := range n.learners {
		n.peers = append(n.peers, id)
	}
}

// ProposeConfChange proposes a single-server membership change. Leader-only,
// and only one may be in flight at a time (a new one is rejected until the
// previous conf-change entry has been applied) so the voter set never shifts
// by more than one member at once — the single-server-change safety rule.
func (n *Node) ProposeConfChange(cc ConfChange) error {
	if n.role != Leader {
		return ErrNotLeader
	}
	if n.pendingConfIndex > n.log.applied {
		return fmt.Errorf("raft: a configuration change is already in flight")
	}
	li := n.log.lastIndex()
	n.log.append(Entry{Type: EntryConfChange, Index: li + 1, Term: n.term, Data: cc.Encode()})
	n.pendingConfIndex = li + 1
	n.maybeCommit()
	n.bcastAppend()
	return nil
}

// IsVoter reports whether id is a voting member.
func (n *Node) IsVoter(id uint64) bool { return n.voters[id] }

// IsLearner reports whether id is a non-voting learner.
func (n *Node) IsLearner(id uint64) bool { return n.learners[id] }

// Voters returns the current voting set size.
func (n *Node) NumVoters() int { return len(n.voters) }
