package cluster

import (
	"context"

	basaltv1 "github.com/LovRanRan/basalt/api/basalt/v1"
	"github.com/LovRanRan/basalt/internal/raft"
)

func toProto(m raft.Message) *basaltv1.RaftMessage {
	pm := &basaltv1.RaftMessage{
		Type:     uint64(m.Type),
		From:     m.From,
		To:       m.To,
		Term:     m.Term,
		LogIndex: m.LogIndex,
		LogTerm:  m.LogTerm,
		Commit:   m.Commit,
		Reject:   m.Reject,
	}
	for _, e := range m.Entries {
		pm.Entries = append(pm.Entries, &basaltv1.RaftEntry{Index: e.Index, Term: e.Term, Data: e.Data})
	}
	return pm
}

func fromProto(pm *basaltv1.RaftMessage) raft.Message {
	m := raft.Message{
		Type:     raft.MessageType(pm.GetType()),
		From:     pm.GetFrom(),
		To:       pm.GetTo(),
		Term:     pm.GetTerm(),
		LogIndex: pm.GetLogIndex(),
		LogTerm:  pm.GetLogTerm(),
		Commit:   pm.GetCommit(),
		Reject:   pm.GetReject(),
	}
	for _, e := range pm.GetEntries() {
		m.Entries = append(m.Entries, raft.Entry{Index: e.GetIndex(), Term: e.GetTerm(), Data: e.GetData()})
	}
	return m
}

// raftServer serves the peer RaftService, feeding messages into the loop.
type raftServer struct {
	basaltv1.UnimplementedRaftServiceServer
	n *Node
}

func (s *raftServer) Step(_ context.Context, req *basaltv1.StepRequest) (*basaltv1.StepResponse, error) {
	pm := req.GetMessage()
	s.n.route(pm.GetGroup(), fromProto(pm))
	return &basaltv1.StepResponse{}, nil
}
