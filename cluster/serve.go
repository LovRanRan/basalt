package cluster

import (
	"net"

	"google.golang.org/grpc"

	basaltv1 "github.com/LovRanRan/basalt/api/basalt/v1"
	"github.com/LovRanRan/basalt/internal/shard"
)

// Servers holds the two gRPC servers a member runs: the peer transport
// (RaftService) and the client API (KVService).
type Servers struct {
	Raft *grpc.Server
	KV   *grpc.Server
}

// Serve registers and starts the member's gRPC services on the given
// listeners. It returns immediately; Stop the returned servers to shut
// down. raftLis carries consensus traffic between nodes; kvLis serves
// clients.
//
// Exactly-once note: writes proposed through a node ride the engine's
// no-dedup namespace — each RPC is one self-contained set/delete batch, so a
// client retry that re-proposes the same bytes is idempotent, and a persisted
// dedup session keyed by an in-memory counter would silently swallow every
// proposal after a node restart (counter resets below the persisted
// high-water mark) while still acknowledging it. Strict end-to-end
// exactly-once needs client-supplied request ids, a follow-up once the proto
// carries them; the state machine's session dedup is reserved for that.
func (n *Node) Serve(raftLis, kvLis net.Listener) *Servers {
	rs := grpc.NewServer()
	basaltv1.RegisterRaftServiceServer(rs, &raftServer{n: n})
	go func() { _ = rs.Serve(raftLis) }()

	ks := grpc.NewServer()
	basaltv1.RegisterKVServiceServer(ks, newKVServer(n))
	go func() { _ = ks.Serve(kvLis) }()

	return &Servers{Raft: rs, KV: ks}
}

// ServeSharded is Serve with a shard-routing KV service: point operations
// route to the group owning the key's slot per the shard map, forwarding to
// the leader node when this node is not it. The KV service is registered on
// BOTH the peer (raft) server and the client server, so a node can forward a
// client request to a peer over the same connection it uses for consensus.
func (n *Node) ServeSharded(raftLis, kvLis net.Listener, smap *shard.ShardMap) *Servers {
	n.setInitialShardMap(smap)
	skv := newShardKV(n)
	rs := grpc.NewServer()
	basaltv1.RegisterRaftServiceServer(rs, &raftServer{n: n})
	basaltv1.RegisterKVServiceServer(rs, skv)
	basaltv1.RegisterShardServiceServer(rs, &shardServer{n: n})
	go func() { _ = rs.Serve(raftLis) }()

	ks := grpc.NewServer()
	basaltv1.RegisterKVServiceServer(ks, skv)
	go func() { _ = ks.Serve(kvLis) }()

	return &Servers{Raft: rs, KV: ks}
}
