package cluster

import (
	"context"
	"errors"
	"sync/atomic"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	basalt "github.com/LovRanRan/basalt"
	basaltv1 "github.com/LovRanRan/basalt/api/basalt/v1"
)

// kvServer serves the client KVService against the cluster: writes are
// proposed through raft, reads go through ReadIndex, and a request to a
// non-leader returns FailedPrecondition carrying the leader hint in the
// status message so the client library can redirect.
type kvServer struct {
	basaltv1.UnimplementedKVServiceServer
	n   *Node
	clk uint64        // this node's client-id namespace for its own proposals
	seq atomic.Uint64 // per-node proposal sequence
}

func newKVServer(n *Node) *kvServer {
	return &kvServer{n: n, clk: n.cfg.ID}
}

// redirect maps an ErrNotLeader to a gRPC status the client can act on: the
// leader hint travels in the Details-free message as "not-leader:<id>".
func redirect(err error) error {
	var nl *ErrNotLeader
	if errors.As(err, &nl) {
		return status.Errorf(codes.FailedPrecondition, "not-leader:%d", nl.Leader)
	}
	return nil
}

func (s *kvServer) command(key, value []byte, del bool) *basalt.Command {
	var b basalt.Batch
	if del {
		b.Delete(key)
	} else {
		b.Put(key, value)
	}
	return &basalt.Command{ClientID: s.clk, Seq: s.seq.Add(1), Batch: b}
}

func validKey(key []byte) error {
	if len(key) == 0 {
		return status.Error(codes.InvalidArgument, "key must not be empty")
	}
	return nil
}

func (s *kvServer) Put(ctx context.Context, req *basaltv1.PutRequest) (*basaltv1.PutResponse, error) {
	if err := validKey(req.GetKey()); err != nil {
		return nil, err
	}
	err := s.n.Propose(ctx, s.command(req.GetKey(), req.GetValue(), false))
	if r := redirect(err); r != nil {
		return nil, r
	}
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	return &basaltv1.PutResponse{}, nil
}

func (s *kvServer) Delete(ctx context.Context, req *basaltv1.DeleteRequest) (*basaltv1.DeleteResponse, error) {
	if err := validKey(req.GetKey()); err != nil {
		return nil, err
	}
	err := s.n.Propose(ctx, s.command(req.GetKey(), nil, true))
	if r := redirect(err); r != nil {
		return nil, r
	}
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	return &basaltv1.DeleteResponse{}, nil
}

func (s *kvServer) Get(ctx context.Context, req *basaltv1.GetRequest) (*basaltv1.GetResponse, error) {
	if err := validKey(req.GetKey()); err != nil {
		return nil, err
	}
	err := s.n.ReadIndex(ctx)
	if r := redirect(err); r != nil {
		return nil, r
	}
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	v, gerr := s.n.DB().Get(req.GetKey())
	if errors.Is(gerr, basalt.ErrNotFound) {
		return &basaltv1.GetResponse{Found: false}, nil
	}
	if gerr != nil {
		return nil, status.Error(codes.Internal, gerr.Error())
	}
	return &basaltv1.GetResponse{Found: true, Value: v}, nil
}
