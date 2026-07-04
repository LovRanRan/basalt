package cluster

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	basalt "github.com/LovRanRan/basalt"
	"github.com/LovRanRan/basalt/internal/raft"
)

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

// sender ships one raft message for a group to its destination peer. The
// host implements it, multiplexing every group over one connection per
// peer and tagging the message with the group id.
type sender func(groupID uint64, m raft.Message)

// group is one raft consensus group with its own engine, storage, and event
// loop. Multiple groups run in one process, each independent; the host
// routes incoming messages here by group id and provides the sender.
type group struct {
	id            uint64
	raft          *raft.Node
	st            *raft.Storage
	db            *basalt.DB
	sm            *basalt.StateMachine
	send          sender
	tickInterval  time.Duration
	snapshotEvery uint64
	dataDir       string

	recvc chan raft.Message
	propc chan *proposal
	readc chan *readReq
	stopc chan struct{}
	done  chan struct{}

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

// openGroup recovers a group's engine and raft state under dataDir (already
// namespaced by group id) and launches its loop.
func openGroup(id, localID uint64, peers []uint64, dataDir string, electionTick, heartbeatTick int, tick time.Duration, snapEvery uint64, snd sender) (*group, error) {
	db, err := basalt.Open(filepath.Join(dataDir, "db"), basalt.Options{DisableWAL: true})
	if err != nil {
		return nil, err
	}
	sm, err := basalt.NewStateMachine(db)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	st, rec, err := raft.OpenStorage(filepath.Join(dataDir, "raft", "state"))
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	// Per-(node,group) RNG: identical default seeds would split every
	// group's vote forever.
	rng := splitmix((id*0x100000001b3 ^ localID) + 1)
	rn := raft.RestoreNode(raft.Config{
		ID: localID, Peers: peers,
		ElectionTick: electionTick, HeartbeatTick: heartbeatTick,
		Rand: func(hi int) int { return int(rng() % uint64(hi)) },
	}, rec, sm.AppliedIndex())

	g := &group{
		id: id, raft: rn, st: st, db: db, sm: sm, send: snd,
		tickInterval: tick, snapshotEvery: snapEvery, dataDir: dataDir,
		recvc:   make(chan raft.Message, 256),
		propc:   make(chan *proposal),
		readc:   make(chan *readReq),
		stopc:   make(chan struct{}),
		done:    make(chan struct{}),
		waiters: map[waiterKey]chan error{},
		reads:   map[uint64]chan error{},
	}
	g.appliedI = sm.AppliedIndex()
	g.snapAt = g.appliedI
	go g.run()
	return g, nil
}

func (g *group) run() {
	defer close(g.done)
	ticker := time.NewTicker(g.tickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-g.stopc:
			return
		case <-ticker.C:
			g.raft.Tick()
		case m := <-g.recvc:
			_ = g.raft.Step(m)
		case p := <-g.propc:
			g.handlePropose(p)
		case r := <-g.readc:
			g.handleRead(r)
		}
		g.drainReady()
	}
}

func (g *group) handlePropose(p *proposal) {
	if g.raft.Role() != raft.Leader {
		p.resp <- &ErrNotLeader{Leader: g.raft.Lead()}
		return
	}
	g.waiters[p.key] = p.resp
	if err := g.raft.Propose(p.data); err != nil {
		delete(g.waiters, p.key)
		p.resp <- err
	}
}

func (g *group) handleRead(r *readReq) {
	if !g.raft.ReadIndex(r.id) {
		r.resp <- &ErrNotLeader{Leader: g.raft.Lead()}
		return
	}
	g.reads[r.id] = r.resp
}

func (g *group) drainReady() {
	for g.raft.HasReady() {
		rd := g.raft.Ready()
		if err := g.st.SaveReady(rd); err != nil {
			g.fail(err)
			return
		}
		applied, err := g.sm.Apply(rd.CommittedEntries)
		if err != nil {
			g.fail(err)
			return
		}
		for _, a := range applied {
			if ch, ok := g.waiters[waiterKey{a.ClientID, a.Seq}]; ok {
				delete(g.waiters, waiterKey{a.ClientID, a.Seq})
				ch <- nil
			}
		}
		if len(rd.CommittedEntries) > 0 {
			g.appliedI = rd.CommittedEntries[len(rd.CommittedEntries)-1].Index
		}
		for _, rs := range rd.ReadStates {
			if ch, ok := g.reads[rs.ID]; ok {
				delete(g.reads, rs.ID)
				ch <- nil
			}
		}
		for _, m := range rd.Messages {
			if m.Type == raft.MsgSnap {
				continue // out-of-band transfer not wired into the RPC path
			}
			g.send(g.id, m)
		}
		g.publishStatus()
		g.raft.Advance(rd)
		g.maybeSnapshot()
	}
}

func (g *group) publishStatus() {
	g.mu.Lock()
	g.leader, g.term, g.role = g.raft.Lead(), g.raft.Term(), g.raft.Role()
	g.mu.Unlock()
}

func (g *group) maybeSnapshot() {
	if g.snapshotEvery == 0 || g.appliedI < g.snapAt+g.snapshotEvery {
		return
	}
	dst := filepath.Join(g.dataDir, fmt.Sprintf("snap-%d", g.appliedI))
	index, _, err := g.sm.Snapshot(dst)
	if err != nil {
		g.fail(err)
		return
	}
	g.raft.CompactTo(index)
	if err := g.st.Rewrite(g.raft.HardStateNow(), g.raft.SnapIndex(), g.raft.SnapTerm(), g.raft.Entries()); err != nil {
		g.fail(err)
		return
	}
	g.snapAt = index
}

func (g *group) fail(err error) {
	panic(fmt.Sprintf("cluster: group %d: %v", g.id, err))
}

func (g *group) Status() (leader, term uint64, isLeader bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.leader, g.term, g.role == raft.Leader
}

func (g *group) step(m raft.Message) {
	select {
	case g.recvc <- m:
	case <-g.stopc:
	}
}

func (g *group) Propose(ctx context.Context, cmd *basalt.Command) error {
	p := &proposal{data: basalt.EncodeCommand(cmd), key: waiterKey{cmd.ClientID, cmd.Seq}, resp: make(chan error, 1)}
	select {
	case g.propc <- p:
	case <-ctx.Done():
		return ctx.Err()
	case <-g.stopc:
		return errors.New("cluster: group stopped")
	}
	select {
	case err := <-p.resp:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-g.stopc:
		return errors.New("cluster: group stopped")
	}
}

func (g *group) ReadIndex(ctx context.Context) error {
	g.mu.Lock()
	g.readSeq++
	id := g.readSeq
	g.mu.Unlock()
	r := &readReq{id: id, resp: make(chan error, 1)}
	select {
	case g.readc <- r:
	case <-ctx.Done():
		return ctx.Err()
	case <-g.stopc:
		return errors.New("cluster: group stopped")
	}
	select {
	case err := <-r.resp:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-g.stopc:
		return errors.New("cluster: group stopped")
	}
}

func (g *group) DB() *basalt.DB { return g.db }

func (g *group) close() error {
	select {
	case <-g.stopc:
	default:
		close(g.stopc)
	}
	<-g.done
	err := g.st.Close()
	if cerr := g.db.Close(); err == nil {
		err = cerr
	}
	return err
}
