package cluster

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	basaltv1 "github.com/LovRanRan/basalt/api/basalt/v1"
	"github.com/LovRanRan/basalt/internal/shard"
)

// shardServer serves the control-plane ShardService: it distributes new
// shard-map versions and bootstraps raft groups on a live node. It rides the
// peer (raft) transport, so a rebalance coordinator reaches every node over
// the same connection consensus uses.
type shardServer struct {
	basaltv1.UnimplementedShardServiceServer
	n *Node
}

// UpdateShardMap adopts a pushed map if it is newer (monotonic epoch). The
// response reports whether it was adopted and the node's epoch afterward, so a
// coordinator can tell whether every node has reached the target version.
func (s *shardServer) UpdateShardMap(_ context.Context, req *basaltv1.UpdateShardMapRequest) (*basaltv1.UpdateShardMapResponse, error) {
	m, err := shard.Unmarshal(req.GetMap())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	accepted := s.n.SetShardMap(m)
	return &basaltv1.UpdateShardMapResponse{Accepted: accepted, Epoch: s.n.ShardMap().Epoch}, nil
}

// GetShardMap returns the node's current map, so a joining node or client can
// fetch the live version.
func (s *shardServer) GetShardMap(_ context.Context, _ *basaltv1.GetShardMapRequest) (*basaltv1.GetShardMapResponse, error) {
	return &basaltv1.GetShardMapResponse{Map: s.n.ShardMap().Marshal()}, nil
}

// AddGroup instantiates a new raft group on the node at runtime.
func (s *shardServer) AddGroup(_ context.Context, req *basaltv1.AddGroupRequest) (*basaltv1.AddGroupResponse, error) {
	if req.GetGroup() == 0 {
		return nil, status.Error(codes.InvalidArgument, "shard: group id must be nonzero")
	}
	if err := s.n.AddGroup(req.GetGroup()); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &basaltv1.AddGroupResponse{}, nil
}

// DistributeShardMap publishes m to the whole cluster: it adopts m locally and
// pushes it to every peer, returning how many nodes (including this one) are at
// or past m's epoch afterward. A rebalance publishes the new map here and waits
// for the count to reach the full membership before proceeding, so every live
// node routes under the new version. That alone does not stop a still-stale
// node from self-serving a moved slot; draining the old owner before the flip
// (P4.7/P4.8) is what closes that window. Unreachable peers are skipped and the
// first error is returned so the caller can retry.
func (n *Node) DistributeShardMap(ctx context.Context, m *shard.ShardMap) (int, error) {
	n.SetShardMap(m)
	atTarget := 0
	if cur := n.ShardMap(); cur != nil && cur.Epoch >= m.Epoch {
		atTarget++
	}
	data := m.Marshal()

	n.mu.RLock()
	peers := make(map[uint64]basaltv1.ShardServiceClient, len(n.shardPeers))
	for id, c := range n.shardPeers {
		peers[id] = c
	}
	n.mu.RUnlock()

	var firstErr error
	for _, c := range peers {
		resp, err := c.UpdateShardMap(ctx, &basaltv1.UpdateShardMapRequest{Map: data})
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if resp.GetEpoch() >= m.Epoch {
			atTarget++
		}
	}
	return atTarget, firstErr
}

// BootstrapGroup instantiates group gid on every node in the cluster at
// runtime — local first, then each peer — so a new raft group elects across
// all members without a restart. The first peer error is returned.
func (n *Node) BootstrapGroup(ctx context.Context, gid uint64) error {
	if err := n.AddGroup(gid); err != nil {
		return err
	}
	n.mu.RLock()
	peers := make(map[uint64]basaltv1.ShardServiceClient, len(n.shardPeers))
	for id, c := range n.shardPeers {
		peers[id] = c
	}
	n.mu.RUnlock()

	var firstErr error
	for _, c := range peers {
		if _, err := c.AddGroup(ctx, &basaltv1.AddGroupRequest{Group: gid}); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
