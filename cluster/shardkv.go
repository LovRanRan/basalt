package cluster

import (
	"context"
	"errors"
	"sync/atomic"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	basalt "github.com/LovRanRan/basalt"
	basaltv1 "github.com/LovRanRan/basalt/api/basalt/v1"
	"github.com/LovRanRan/basalt/internal/shard"
)

// shardKV serves the client KVService against a sharded cluster: a key's
// slot selects its raft group, and the request is proposed through (or read
// from) that group if this node hosts its leader — otherwise it returns a
// not-leader redirect. Cross-group ordered Scan is the router's job (P4.4);
// this server handles point operations.
type shardKV struct {
	basaltv1.UnimplementedKVServiceServer
	n    *Node
	smap *shard.ShardMap
	seq  perNodeSeq
}

type perNodeSeq struct {
	client uint64
	seq    atomic.Uint64
}

func newShardKV(n *Node, smap *shard.ShardMap) *shardKV {
	return &shardKV{n: n, smap: smap, seq: perNodeSeq{client: n.cfg.ID}}
}

// groupFor returns the local group that owns key, or a not-leader-style
// error the client should redirect on when this node does not host it (it
// does host every group in the all-nodes-all-groups placement, but a
// future partial placement would redirect).
func (s *shardKV) groupFor(key []byte) (*group, error) {
	gid := s.smap.Lookup(key)
	if gid == 0 {
		return nil, status.Error(codes.FailedPrecondition, "shard: key maps to an unassigned slot")
	}
	g := s.n.Group(gid)
	if g == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "not-leader:0")
	}
	return g, nil
}

func (s *shardKV) command(key, value []byte, del bool) *basalt.Command {
	var b basalt.Batch
	if del {
		b.Delete(key)
	} else {
		b.Put(key, value)
	}
	return &basalt.Command{ClientID: s.seq.client, Seq: s.seq.seq.Add(1), Batch: b}
}

func (s *shardKV) Put(ctx context.Context, req *basaltv1.PutRequest) (*basaltv1.PutResponse, error) {
	if err := validKey(req.GetKey()); err != nil {
		return nil, err
	}
	g, err := s.groupFor(req.GetKey())
	if err != nil {
		return nil, err
	}
	perr := g.Propose(ctx, s.command(req.GetKey(), req.GetValue(), false))
	if r := redirect(perr); r != nil {
		return nil, r
	}
	if perr != nil {
		return nil, status.Error(codes.Unavailable, perr.Error())
	}
	return &basaltv1.PutResponse{}, nil
}

func (s *shardKV) Delete(ctx context.Context, req *basaltv1.DeleteRequest) (*basaltv1.DeleteResponse, error) {
	if err := validKey(req.GetKey()); err != nil {
		return nil, err
	}
	g, err := s.groupFor(req.GetKey())
	if err != nil {
		return nil, err
	}
	perr := g.Propose(ctx, s.command(req.GetKey(), nil, true))
	if r := redirect(perr); r != nil {
		return nil, r
	}
	if perr != nil {
		return nil, status.Error(codes.Unavailable, perr.Error())
	}
	return &basaltv1.DeleteResponse{}, nil
}

func (s *shardKV) Get(ctx context.Context, req *basaltv1.GetRequest) (*basaltv1.GetResponse, error) {
	if err := validKey(req.GetKey()); err != nil {
		return nil, err
	}
	g, err := s.groupFor(req.GetKey())
	if err != nil {
		return nil, err
	}
	rerr := g.ReadIndex(ctx)
	if r := redirect(rerr); r != nil {
		return nil, r
	}
	if rerr != nil {
		return nil, status.Error(codes.Unavailable, rerr.Error())
	}
	v, gerr := g.DB().Get(req.GetKey())
	if errors.Is(gerr, basalt.ErrNotFound) {
		return &basaltv1.GetResponse{Found: false}, nil
	}
	if gerr != nil {
		return nil, status.Error(codes.Internal, gerr.Error())
	}
	return &basaltv1.GetResponse{Found: true, Value: v}, nil
}
