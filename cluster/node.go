// Package cluster runs a Basalt cluster member: one or more independent
// raft groups per process behind a group manager, each with its own engine,
// storage, and event loop. A single gRPC connection per peer multiplexes
// consensus traffic for every group, tagged by group id. Writes propose
// through a group and are acknowledged on apply; reads go through ReadIndex.
// A request to a non-leader returns NotLeader with the leader hint so the
// client can redirect.
package cluster

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	basalt "github.com/LovRanRan/basalt"
	basaltv1 "github.com/LovRanRan/basalt/api/basalt/v1"
	"github.com/LovRanRan/basalt/internal/raft"
	"github.com/LovRanRan/basalt/internal/shard"
)

// ErrNotLeader is returned when a request reaches a non-leader; Leader (if
// nonzero) names the node to retry against.
type ErrNotLeader struct{ Leader uint64 }

func (e *ErrNotLeader) Error() string {
	if e.Leader == 0 {
		return "cluster: not the leader (no leader known)"
	}
	return fmt.Sprintf("cluster: not the leader (try node %d)", e.Leader)
}

// Config describes a member and its peers (id -> host:port for the raft
// transport). Peers includes this node.
type Config struct {
	ID            uint64
	Peers         map[uint64]string
	DataDir       string
	ElectionTick  int
	HeartbeatTick int
	TickInterval  time.Duration // wall-clock per raft tick; default 20ms
	SnapshotEvery uint64        // compact after this many applied entries; 0 disables
	// Groups is the set of raft group ids this member hosts. Empty means a
	// single group with id 1 (the degenerate one-group case).
	Groups []uint64
}

type proposal struct {
	data []byte
	key  waiterKey
	resp chan error
}

type readReq struct {
	id   uint64
	resp chan error
}

type waiterKey struct{ client, seq uint64 }

// Node is a running cluster member: a group manager hosting one or more raft
// groups over a shared peer transport.
type Node struct {
	cfg     Config
	peerIDs []uint64

	mu     sync.RWMutex
	groups map[uint64]*group
	closed bool // set by Close; AddGroup refuses to install into a closed node

	// addMu serializes AddGroup so two concurrent adds of the same group can
	// never both open the engine (whose exclusive flock would fail the loser)
	// — held across the open, which does I/O, so it must not be n.mu.
	addMu sync.Mutex

	peers      map[uint64]basaltv1.RaftServiceClient
	kvPeers    map[uint64]basaltv1.KVServiceClient    // for forwarding client requests
	shardPeers map[uint64]basaltv1.ShardServiceClient // for shard-map distribution
	conns      []*grpc.ClientConn

	// smap is the active shard map, swapped atomically so routing always
	// reads the freshest version. Adoption is monotonic in Epoch (SetShardMap)
	// so a stale map can never overwrite a fresher one — the epoch fence.
	smap atomic.Pointer[shard.ShardMap]
}

// Open starts a member: it dials peers, opens every configured raft group
// (each under a group-namespaced data directory), and returns the manager.
// Serve wires it to gRPC servers.
func Open(cfg Config) (*Node, error) {
	if cfg.TickInterval <= 0 {
		cfg.TickInterval = 20 * time.Millisecond
	}
	if cfg.ElectionTick <= 0 {
		cfg.ElectionTick = 10
	}
	if len(cfg.Groups) == 0 {
		cfg.Groups = []uint64{1}
	}
	peerIDs := make([]uint64, 0, len(cfg.Peers))
	for id := range cfg.Peers {
		peerIDs = append(peerIDs, id)
	}
	n := &Node{
		cfg: cfg, peerIDs: peerIDs,
		groups:     map[uint64]*group{},
		peers:      map[uint64]basaltv1.RaftServiceClient{},
		kvPeers:    map[uint64]basaltv1.KVServiceClient{},
		shardPeers: map[uint64]basaltv1.ShardServiceClient{},
	}
	n.smap.Store(shard.NewShardMap(cfg.Groups))
	for id, addr := range cfg.Peers {
		if id == cfg.ID {
			continue
		}
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			n.closeConns()
			return nil, err
		}
		n.conns = append(n.conns, conn)
		n.peers[id] = basaltv1.NewRaftServiceClient(conn)
		n.kvPeers[id] = basaltv1.NewKVServiceClient(conn)
		n.shardPeers[id] = basaltv1.NewShardServiceClient(conn)
	}
	for _, gid := range cfg.Groups {
		g, err := openGroup(gid, cfg.ID, peerIDs, n.groupDir(gid),
			cfg.ElectionTick, cfg.HeartbeatTick, cfg.TickInterval, cfg.SnapshotEvery, n.sendTo)
		if err != nil {
			n.closeGroups()
			n.closeConns()
			return nil, err
		}
		n.groups[gid] = g
	}
	return n, nil
}

// groupDir namespaces a group's on-disk state so groups never collide.
func (n *Node) groupDir(gid uint64) string {
	return filepath.Join(n.cfg.DataDir, fmt.Sprintf("group-%d", gid))
}

// sendTo ships one group's raft message to its destination peer, tagged
// with the group id, over the shared connection. Undeliverable messages are
// dropped (raft retries).
func (n *Node) sendTo(groupID uint64, m raft.Message) {
	n.mu.RLock()
	peer, ok := n.peers[m.To]
	n.mu.RUnlock()
	if !ok {
		return
	}
	pm := toProto(m)
	pm.Group = groupID
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, _ = peer.Step(ctx, &basaltv1.StepRequest{Message: pm})
	}()
}

// route delivers an incoming raft message to its group.
func (n *Node) route(groupID uint64, m raft.Message) {
	n.mu.RLock()
	g := n.groups[groupID]
	n.mu.RUnlock()
	if g != nil {
		g.step(m)
	}
}

// Group returns the hosted group with the given id, or nil.
func (n *Node) Group(id uint64) *group {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.groups[id]
}

// GroupIDs returns the ids of the groups this member hosts.
func (n *Node) GroupIDs() []uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	out := make([]uint64, 0, len(n.groups))
	for id := range n.groups {
		out = append(out, id)
	}
	return out
}

// ShardMap returns the active shard map (never nil once Open has run).
func (n *Node) ShardMap() *shard.ShardMap { return n.smap.Load() }

// setInitialShardMap installs the authoritative starting map unconditionally
// (used at Serve time, before any distribution has happened).
func (n *Node) setInitialShardMap(m *shard.ShardMap) { n.smap.Store(m) }

// SetShardMap adopts m only if its epoch is strictly greater than the current
// map's — monotonic, so a stale (delayed or replayed) map can never overwrite
// a fresher one. This is the epoch fence at the adoption boundary. It returns
// true if m was adopted.
func (n *Node) SetShardMap(m *shard.ShardMap) bool {
	for {
		cur := n.smap.Load()
		if cur != nil && m.Epoch <= cur.Epoch {
			return false
		}
		if n.smap.CompareAndSwap(cur, m) {
			return true
		}
	}
}

// AddGroup instantiates a brand-new raft group on this already-running node,
// so a cluster can grow groups without a restart (a rebalance may assign slots
// to a group this node must now host). It is idempotent: adding a group the
// node already hosts is a no-op. addMu serializes concurrent adds so the loser
// of a same-group race sees the group already present rather than colliding on
// the engine flock; a group opened concurrently with Close is closed and
// rejected so it can never leak past shutdown.
func (n *Node) AddGroup(gid uint64) error {
	if gid == 0 {
		return fmt.Errorf("cluster: group id must be nonzero")
	}
	n.addMu.Lock()
	defer n.addMu.Unlock()

	n.mu.RLock()
	_, exists := n.groups[gid]
	closed := n.closed
	n.mu.RUnlock()
	if closed {
		return fmt.Errorf("cluster: node is closed")
	}
	if exists {
		return nil
	}

	g, err := openGroup(gid, n.cfg.ID, n.peerIDs, n.groupDir(gid),
		n.cfg.ElectionTick, n.cfg.HeartbeatTick, n.cfg.TickInterval, n.cfg.SnapshotEvery, n.sendTo)
	if err != nil {
		return err
	}
	n.mu.Lock()
	if n.closed { // Close ran during the open: don't leak the fresh group
		n.mu.Unlock()
		_ = g.close()
		return fmt.Errorf("cluster: node is closed")
	}
	n.groups[gid] = g // absent for certain: addMu makes us the only installer
	n.mu.Unlock()
	return nil
}

// only returns the sole group when the member hosts exactly one, for the
// degenerate single-group API.
func (n *Node) only() *group {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if len(n.groups) != 1 {
		panic("cluster: single-group API used on a multi-group node")
	}
	for _, g := range n.groups {
		return g
	}
	return nil
}

// Single-group convenience methods delegate to the sole group.
func (n *Node) Status() (uint64, uint64, bool) { return n.only().Status() }
func (n *Node) Propose(ctx context.Context, cmd *basalt.Command) error {
	return n.only().Propose(ctx, cmd)
}
func (n *Node) ReadIndex(ctx context.Context) error { return n.only().ReadIndex(ctx) }
func (n *Node) DB() *basalt.DB                      { return n.only().DB() }

func (n *Node) closeConns() {
	for _, c := range n.conns {
		_ = c.Close()
	}
}

func (n *Node) closeGroups() {
	for _, g := range n.groups {
		_ = g.close()
	}
}

// Close stops every group and releases the shared transport.
func (n *Node) Close() error {
	var err error
	n.mu.Lock()
	n.closed = true
	groups := n.groups
	n.groups = map[uint64]*group{}
	n.mu.Unlock()
	for _, g := range groups {
		if cerr := g.close(); err == nil {
			err = cerr
		}
	}
	n.closeConns()
	return err
}
