// Package cluster runs one Basalt cluster member: a raft node driven by a
// single event loop, an LSM engine as its state machine, a gRPC peer
// transport for consensus messages, and the client KV service. Writes are
// proposed through raft and acknowledged on apply; reads go through
// ReadIndex. A request to a non-leader returns NotLeader with the current
// leader hint so the client can redirect.
package cluster

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	basalt "github.com/LovRanRan/basalt"
	basaltv1 "github.com/LovRanRan/basalt/api/basalt/v1"
	"github.com/LovRanRan/basalt/internal/raft"
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

// Node is a running cluster member.
type Node struct {
	cfg  Config
	raft *raft.Node
	st   *raft.Storage
	db   *basalt.DB
	sm   *basalt.StateMachine

	recvc chan raft.Message
	propc chan *proposal
	readc chan *readReq
	stopc chan struct{}
	done  chan struct{}

	peers map[uint64]basaltv1.RaftServiceClient
	conns []*grpc.ClientConn

	mu       sync.Mutex
	leader   uint64
	term     uint64
	role     raft.Role
	waiters  map[waiterKey]chan error
	reads    map[uint64]chan error
	readSeq  uint64
	appliedI uint64
	snapAt   uint64
}

// Open starts a member: it recovers the engine and raft state, dials peers,
// and launches the event loop. Serve wires it to gRPC servers.
func Open(cfg Config) (*Node, error) {
	if cfg.TickInterval <= 0 {
		cfg.TickInterval = 20 * time.Millisecond
	}
	if cfg.ElectionTick <= 0 {
		cfg.ElectionTick = 10
	}
	db, err := basalt.Open(filepath.Join(cfg.DataDir, "db"), basalt.Options{DisableWAL: true})
	if err != nil {
		return nil, err
	}
	sm, err := basalt.NewStateMachine(db)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	st, rec, err := raft.OpenStorage(filepath.Join(cfg.DataDir, "raft", "state"))
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	ids := make([]uint64, 0, len(cfg.Peers))
	for id := range cfg.Peers {
		ids = append(ids, id)
	}
	// Per-node RNG: without distinct seeds every node draws identical
	// election timeouts and splits the vote forever.
	rng := splitmix(cfg.ID*0x9e3779b97f4a7c15 + 1)
	rn := raft.RestoreNode(raft.Config{
		ID: cfg.ID, Peers: ids,
		ElectionTick: cfg.ElectionTick, HeartbeatTick: cfg.HeartbeatTick,
		Rand: func(hi int) int { return int(rng() % uint64(hi)) },
	}, rec, sm.AppliedIndex())

	n := &Node{
		cfg: cfg, raft: rn, st: st, db: db, sm: sm,
		recvc:   make(chan raft.Message, 256),
		propc:   make(chan *proposal),
		readc:   make(chan *readReq),
		stopc:   make(chan struct{}),
		done:    make(chan struct{}),
		peers:   map[uint64]basaltv1.RaftServiceClient{},
		waiters: map[waiterKey]chan error{},
		reads:   map[uint64]chan error{},
	}
	n.appliedI = sm.AppliedIndex()
	n.snapAt = n.appliedI
	for id, addr := range cfg.Peers {
		if id == cfg.ID {
			continue
		}
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			n.closeConns()
			_ = st.Close()
			_ = db.Close()
			return nil, err
		}
		n.conns = append(n.conns, conn)
		n.peers[id] = basaltv1.NewRaftServiceClient(conn)
	}
	go n.run()
	return n, nil
}

func splitmix(seed uint64) func() uint64 {
	s := seed
	return func() uint64 {
		s += 0x9e3779b97f4a7c15
		z := s
		z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
		z = (z ^ (z >> 27)) * 0x94d049bb133111eb
		return z ^ (z >> 31)
	}
}

func (n *Node) closeConns() {
	for _, c := range n.conns {
		_ = c.Close()
	}
}

// run is the single event loop that owns the raft node.
func (n *Node) run() {
	defer close(n.done)
	ticker := time.NewTicker(n.cfg.TickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-n.stopc:
			return
		case <-ticker.C:
			n.raft.Tick()
		case m := <-n.recvc:
			_ = n.raft.Step(m)
		case p := <-n.propc:
			n.handlePropose(p)
		case r := <-n.readc:
			n.handleRead(r)
		}
		n.drainReady()
	}
}

func (n *Node) handlePropose(p *proposal) {
	if n.raft.Role() != raft.Leader {
		p.resp <- &ErrNotLeader{Leader: n.raft.Lead()}
		return
	}
	n.waiters[p.key] = p.resp
	if err := n.raft.Propose(p.data); err != nil {
		delete(n.waiters, p.key)
		p.resp <- err
	}
}

func (n *Node) handleRead(r *readReq) {
	if !n.raft.ReadIndex(r.id) {
		r.resp <- &ErrNotLeader{Leader: n.raft.Lead()}
		return
	}
	n.reads[r.id] = r.resp
}

func (n *Node) drainReady() {
	for n.raft.HasReady() {
		rd := n.raft.Ready()
		if err := n.st.SaveReady(rd); err != nil {
			n.fail(err)
			return
		}
		applied, err := n.sm.Apply(rd.CommittedEntries)
		if err != nil {
			n.fail(err)
			return
		}
		for _, a := range applied {
			if ch, ok := n.waiters[waiterKey{a.ClientID, a.Seq}]; ok {
				delete(n.waiters, waiterKey{a.ClientID, a.Seq})
				ch <- nil
			}
		}
		if len(rd.CommittedEntries) > 0 {
			n.appliedI = rd.CommittedEntries[len(rd.CommittedEntries)-1].Index
		}
		for _, rs := range rd.ReadStates {
			if ch, ok := n.reads[rs.ID]; ok {
				delete(n.reads, rs.ID)
				ch <- nil
			}
		}
		n.send(rd.Messages)
		n.publishStatus()
		n.raft.Advance(rd)
		n.maybeSnapshot()
	}
}

// publishStatus snapshots leader/term/role for concurrent readers of Status.
func (n *Node) publishStatus() {
	n.mu.Lock()
	n.leader, n.term, n.role = n.raft.Lead(), n.raft.Term(), n.raft.Role()
	n.mu.Unlock()
}

// maybeSnapshot compacts the raft log once enough entries have applied.
func (n *Node) maybeSnapshot() {
	if n.cfg.SnapshotEvery == 0 || n.appliedI < n.snapAt+n.cfg.SnapshotEvery {
		return
	}
	dst := filepath.Join(n.cfg.DataDir, fmt.Sprintf("snap-%d", n.appliedI))
	index, term, err := n.sm.Snapshot(dst)
	if err != nil {
		n.fail(err)
		return
	}
	n.raft.CompactTo(index)
	if err := n.st.Rewrite(n.raft.HardStateNow(), n.raft.SnapIndex(), n.raft.SnapTerm(), n.raft.Entries()); err != nil {
		n.fail(err)
		return
	}
	n.snapAt = index
	_ = term
}

// send ships raft messages to peers, dropping any this loop cannot deliver
// (raft retries). MsgSnap is not yet wired to a real transfer here; a
// follower that far behind waits for a future InstallSnapshot RPC.
func (n *Node) send(msgs []raft.Message) {
	for _, m := range msgs {
		if m.Type == raft.MsgSnap {
			continue // out-of-band transfer not wired into the RPC path yet
		}
		peer, ok := n.peers[m.To]
		if !ok {
			continue
		}
		go func(peer basaltv1.RaftServiceClient, m raft.Message) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_, _ = peer.Step(ctx, &basaltv1.StepRequest{Message: toProto(m)})
		}(peer, m)
	}
}

func (n *Node) fail(err error) {
	// A background failure poisons the loop; callers surface it as
	// Unavailable. Log via panic in tests; production would use slog.
	panic(fmt.Sprintf("cluster: node %d: %v", n.cfg.ID, err))
}

// Status reports the node's view of leadership.
func (n *Node) Status() (leader uint64, term uint64, isLeader bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leader, n.term, n.role == raft.Leader
}

// stepMessage feeds an incoming raft message into the loop.
func (n *Node) stepMessage(m raft.Message) {
	select {
	case n.recvc <- m:
	case <-n.stopc:
	}
}

// Propose submits a command and waits for it to apply (or fail). ctx bounds
// the wait.
func (n *Node) Propose(ctx context.Context, cmd *basalt.Command) error {
	p := &proposal{data: basalt.EncodeCommand(cmd), key: waiterKey{cmd.ClientID, cmd.Seq}, resp: make(chan error, 1)}
	select {
	case n.propc <- p:
	case <-ctx.Done():
		return ctx.Err()
	case <-n.stopc:
		return errors.New("cluster: node stopped")
	}
	select {
	case err := <-p.resp:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-n.stopc:
		return errors.New("cluster: node stopped")
	}
}

// ReadIndex confirms a linearizable read; on success the caller may read
// the engine.
func (n *Node) ReadIndex(ctx context.Context) error {
	n.mu.Lock()
	n.readSeq++
	id := n.readSeq
	n.mu.Unlock()
	r := &readReq{id: id, resp: make(chan error, 1)}
	select {
	case n.readc <- r:
	case <-ctx.Done():
		return ctx.Err()
	case <-n.stopc:
		return errors.New("cluster: node stopped")
	}
	select {
	case err := <-r.resp:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-n.stopc:
		return errors.New("cluster: node stopped")
	}
}

// DB exposes the engine for local reads after a confirmed ReadIndex.
func (n *Node) DB() *basalt.DB { return n.db }

// Close stops the loop and releases resources.
func (n *Node) Close() error {
	select {
	case <-n.stopc:
	default:
		close(n.stopc)
	}
	<-n.done
	n.closeConns()
	err := n.st.Close()
	if cerr := n.db.Close(); err == nil {
		err = cerr
	}
	return err
}
