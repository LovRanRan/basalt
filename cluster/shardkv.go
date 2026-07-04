package cluster

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sort"
	"strconv"
	"sync/atomic"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	basalt "github.com/LovRanRan/basalt"
	basaltv1 "github.com/LovRanRan/basalt/api/basalt/v1"
)

// routeGIDMD is the gRPC metadata key a forwarding node stamps with the group
// it routed a key to. The terminal node compares it against its own map: if
// they disagree, one side's map is stale, so it rejects (retryably) rather
// than serve a key its map assigns elsewhere. Comparing the routed slot's
// owner — not the whole-map epoch — means an unrelated slot moving never
// spuriously fences a key whose ownership is unchanged.
//
// This fences the *forwarded* hop only. A direct client call to a stale node
// that still believes it owns a moved slot carries no header and self-routes,
// so single-owner safety across a live slot handoff is NOT provided here — it
// is the job of the rebalance drain (P4.7/P4.8), which stops the old owner
// before the map flips.
const routeGIDMD = "basalt-route-gid"

// scanBatch is how many pairs one streamed ScanResponse carries.
const scanBatch = 256

// shardKV is the sharded front door. Any node can serve any request: a
// point op routes to the group owning the key's slot and, if this node does
// not lead that group, forwards to the node that does; Scan fans out across
// every group, pulling each group's slice from its leader and merge-sorting
// into one globally ordered stream. A short redirect protocol handles
// leadership changing mid-flight.
type shardKV struct {
	basaltv1.UnimplementedKVServiceServer
	n      *Node
	client uint64
	seq    atomic.Uint64
}

func newShardKV(n *Node) *shardKV {
	return &shardKV{n: n, client: n.cfg.ID}
}

// fenceRoute rejects a forwarded request when the forwarder and this node
// disagree about which group owns the key — a sign one side's map is stale.
// localGID is this node's own lookup for the key. A request without the header
// (a direct client call) is never fenced. Returns a retryable Unavailable so
// the client tries again once maps converge.
func (s *shardKV) fenceRoute(ctx context.Context, localGID uint64) error {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil
	}
	vals := md.Get(routeGIDMD)
	if len(vals) == 0 {
		return nil
	}
	routed, err := strconv.ParseUint(vals[0], 10, 64)
	if err != nil {
		return nil
	}
	if routed != localGID {
		return status.Errorf(codes.Unavailable,
			"shard: ownership disagreement (forwarded to group %d, local map says %d)", routed, localGID)
	}
	return nil
}

// forwardCtx stamps the outgoing context with the group this node routed the
// key to, so the receiving node can fence a stale hop.
func (s *shardKV) forwardCtx(ctx context.Context, gid uint64) context.Context {
	return metadata.AppendToOutgoingContext(ctx, routeGIDMD, strconv.FormatUint(gid, 10))
}

// leaderOf returns the node currently leading a group, or 0 if unknown.
func (s *shardKV) leaderOf(gid uint64) uint64 {
	g := s.n.Group(gid)
	if g == nil {
		return 0
	}
	l, _, _ := g.Status()
	return l
}

func (s *shardKV) command(key, value []byte, del bool) *basalt.Command {
	var b basalt.Batch
	if del {
		b.Delete(key)
	} else {
		b.Put(key, value)
	}
	return &basalt.Command{ClientID: s.client, Seq: s.seq.Add(1), Batch: b}
}

func (s *shardKV) Put(ctx context.Context, req *basaltv1.PutRequest) (*basaltv1.PutResponse, error) {
	if err := validKey(req.GetKey()); err != nil {
		return nil, err
	}
	gid := s.n.ShardMap().Lookup(req.GetKey())
	if gid == 0 {
		return nil, status.Error(codes.FailedPrecondition, "shard: key maps to an unassigned slot")
	}
	if err := s.fenceRoute(ctx, gid); err != nil {
		return nil, err
	}
	g := s.n.Group(gid)
	if g == nil || s.leaderOf(gid) != s.n.cfg.ID {
		// Not the leader of the owning group: forward to the leader node.
		if peer, ok := s.forwardTarget(gid); ok {
			return peer.Put(s.forwardCtx(ctx, gid), req)
		}
		return nil, status.Errorf(codes.Unavailable, "shard: no leader for group %d", gid)
	}
	perr := g.Propose(ctx, s.command(req.GetKey(), req.GetValue(), false))
	if perr != nil {
		return nil, mapErr(perr)
	}
	return &basaltv1.PutResponse{}, nil
}

func (s *shardKV) Delete(ctx context.Context, req *basaltv1.DeleteRequest) (*basaltv1.DeleteResponse, error) {
	if err := validKey(req.GetKey()); err != nil {
		return nil, err
	}
	gid := s.n.ShardMap().Lookup(req.GetKey())
	if gid == 0 {
		return nil, status.Error(codes.FailedPrecondition, "shard: key maps to an unassigned slot")
	}
	if err := s.fenceRoute(ctx, gid); err != nil {
		return nil, err
	}
	g := s.n.Group(gid)
	if g == nil || s.leaderOf(gid) != s.n.cfg.ID {
		if peer, ok := s.forwardTarget(gid); ok {
			return peer.Delete(s.forwardCtx(ctx, gid), req)
		}
		return nil, status.Errorf(codes.Unavailable, "shard: no leader for group %d", gid)
	}
	if perr := g.Propose(ctx, s.command(req.GetKey(), nil, true)); perr != nil {
		return nil, mapErr(perr)
	}
	return &basaltv1.DeleteResponse{}, nil
}

func (s *shardKV) Get(ctx context.Context, req *basaltv1.GetRequest) (*basaltv1.GetResponse, error) {
	if err := validKey(req.GetKey()); err != nil {
		return nil, err
	}
	gid := s.n.ShardMap().Lookup(req.GetKey())
	if gid == 0 {
		return nil, status.Error(codes.FailedPrecondition, "shard: key maps to an unassigned slot")
	}
	if err := s.fenceRoute(ctx, gid); err != nil {
		return nil, err
	}
	g := s.n.Group(gid)
	if g == nil || s.leaderOf(gid) != s.n.cfg.ID {
		if peer, ok := s.forwardTarget(gid); ok {
			return peer.Get(s.forwardCtx(ctx, gid), req)
		}
		return nil, status.Errorf(codes.Unavailable, "shard: no leader for group %d", gid)
	}
	if rerr := g.ReadIndex(ctx); rerr != nil {
		return nil, mapErr(rerr)
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

// forwardTarget returns the KV client for the node leading gid, or false.
func (s *shardKV) forwardTarget(gid uint64) (basaltv1.KVServiceClient, bool) {
	leader := s.leaderOf(gid)
	if leader == 0 || leader == s.n.cfg.ID {
		return nil, false
	}
	s.n.mu.RLock()
	peer, ok := s.n.kvPeers[leader]
	s.n.mu.RUnlock()
	return peer, ok
}

// Scan fans out. A group-scoped request (Group != 0) scans exactly that
// group on its leader; a client-facing request (Group == 0) pulls every
// group's slice — locally or forwarded to each leader — and merge-sorts.
func (s *shardKV) Scan(req *basaltv1.ScanRequest, stream basaltv1.KVService_ScanServer) error {
	if req.GetGroup() != 0 {
		return s.scanGroup(req, stream)
	}
	return s.scanAllGroups(req, stream)
}

// scanGroup serves one group's slice, ReadIndex-consistent, on its leader.
func (s *shardKV) scanGroup(req *basaltv1.ScanRequest, stream basaltv1.KVService_ScanServer) error {
	gid := req.GetGroup()
	g := s.n.Group(gid)
	if g == nil || s.leaderOf(gid) != s.n.cfg.ID {
		return status.Errorf(codes.FailedPrecondition, "shard: group %d not led here", gid)
	}
	if err := g.ReadIndex(stream.Context()); err != nil {
		return mapErr(err)
	}
	var start, end []byte
	if len(req.GetStart()) > 0 {
		start = req.GetStart()
	}
	if len(req.GetEnd()) > 0 {
		end = req.GetEnd()
	}
	it := g.DB().Scan(start, end)
	defer it.Close()
	batch := make([]*basaltv1.KeyValue, 0, scanBatch)
	for ; it.Valid(); it.Next() {
		batch = append(batch, &basaltv1.KeyValue{
			Key:   append([]byte(nil), it.Key()...),
			Value: append([]byte(nil), it.Value()...),
		})
		if len(batch) == scanBatch {
			if err := stream.Send(&basaltv1.ScanResponse{Pairs: batch}); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}
	if err := it.Error(); err != nil {
		return mapErr(err)
	}
	if len(batch) > 0 {
		return stream.Send(&basaltv1.ScanResponse{Pairs: batch})
	}
	return nil
}

// scanAllGroups is the coordinator: gather every group's pairs, merge-sort
// globally, apply the range and limit, and stream. Hashing destroys range
// locality, so a full scan must touch every group.
func (s *shardKV) scanAllGroups(req *basaltv1.ScanRequest, stream basaltv1.KVService_ScanServer) error {
	ctx := stream.Context()
	var all []*basaltv1.KeyValue
	for _, gid := range s.n.ShardMap().Groups() {
		pairs, err := s.gatherGroup(ctx, gid, req)
		if err != nil {
			return err
		}
		all = append(all, pairs...)
	}
	sort.Slice(all, func(i, j int) bool { return bytes.Compare(all[i].GetKey(), all[j].GetKey()) < 0 })
	if req.GetLimit() > 0 && uint64(len(all)) > req.GetLimit() {
		all = all[:req.GetLimit()]
	}
	for i := 0; i < len(all); i += scanBatch {
		end := i + scanBatch
		if end > len(all) {
			end = len(all)
		}
		if err := stream.Send(&basaltv1.ScanResponse{Pairs: all[i:end]}); err != nil {
			return err
		}
	}
	return nil
}

// gatherGroup collects one group's pairs, locally if we lead it else from
// the leader node.
func (s *shardKV) gatherGroup(ctx context.Context, gid uint64, req *basaltv1.ScanRequest) ([]*basaltv1.KeyValue, error) {
	sub := &basaltv1.ScanRequest{Start: req.GetStart(), End: req.GetEnd(), Group: gid}
	var scanStream basaltv1.KVService_ScanClient
	if s.leaderOf(gid) == s.n.cfg.ID {
		return s.gatherLocal(ctx, gid, req)
	}
	peer, ok := s.forwardTarget(gid)
	if !ok {
		return nil, status.Errorf(codes.Unavailable, "shard: no leader for group %d", gid)
	}
	var err error
	scanStream, err = peer.Scan(ctx, sub)
	if err != nil {
		return nil, err
	}
	var out []*basaltv1.KeyValue
	for {
		resp, err := scanStream.Recv()
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		out = append(out, resp.GetPairs()...)
	}
}

// gatherLocal scans a group this node leads, without a network hop.
func (s *shardKV) gatherLocal(ctx context.Context, gid uint64, req *basaltv1.ScanRequest) ([]*basaltv1.KeyValue, error) {
	g := s.n.Group(gid)
	if err := g.ReadIndex(ctx); err != nil {
		return nil, mapErr(err)
	}
	var start, end []byte
	if len(req.GetStart()) > 0 {
		start = req.GetStart()
	}
	if len(req.GetEnd()) > 0 {
		end = req.GetEnd()
	}
	it := g.DB().Scan(start, end)
	defer it.Close()
	var out []*basaltv1.KeyValue
	for ; it.Valid(); it.Next() {
		out = append(out, &basaltv1.KeyValue{
			Key:   append([]byte(nil), it.Key()...),
			Value: append([]byte(nil), it.Value()...),
		})
	}
	return out, it.Error()
}

// mapErr turns a group error into a client status: a not-leader redirect
// (leadership changed mid-flight) or Unavailable.
func mapErr(err error) error {
	var nl *ErrNotLeader
	if errors.As(err, &nl) {
		return status.Errorf(codes.FailedPrecondition, "not-leader:%d", nl.Leader)
	}
	return status.Error(codes.Unavailable, err.Error())
}
