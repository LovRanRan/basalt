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
	fsyncDelay    time.Duration // fault injection: stall persistence (slow disk)
	dataDir       string

	recvc  chan raft.Message
	propc  chan *proposal
	readc  chan *readReq
	adminc chan *adminReq
	stopc  chan struct{}
	done   chan struct{}

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
func openGroup(id, localID uint64, peers []uint64, dataDir string, electionTick, heartbeatTick int, tick time.Duration, snapEvery uint64, fsyncDelay time.Duration, snd sender) (*group, error) {
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
		tickInterval: tick, snapshotEvery: snapEvery, fsyncDelay: fsyncDelay, dataDir: dataDir,
		recvc:   make(chan raft.Message, 256),
		propc:   make(chan *proposal),
		readc:   make(chan *readReq),
		adminc:  make(chan *adminReq),
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
		case a := <-g.adminc:
			a.resp <- a.fn()
		}
		g.drainReady()
	}
}

func (g *group) handlePropose(p *proposal) {
	if g.raft.Role() != raft.Leader {
		p.resp <- &ErrNotLeader{Leader: g.raft.Lead()}
		return
	}
	if p.guard != nil {
		if err := p.guard(); err != nil {
			p.resp <- err
			return
		}
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
		// Injected slow disk: stall only Readies that actually persist
		// (entries or a HardState change) — message-only Readies cost a real
		// fsync-slow disk nothing, and stalling them would throttle the whole
		// node instead of its disk.
		if g.fsyncDelay > 0 && (len(rd.Entries) > 0 || rd.HardState != nil) {
			time.Sleep(g.fsyncDelay)
		}
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
	role := g.raft.Role()
	lead := g.raft.Lead()
	g.mu.Lock()
	g.leader, g.term, g.role = lead, g.raft.Term(), role
	g.mu.Unlock()
	// Stepping down strands any proposal or read waiter whose entry has not
	// yet committed: it was appended under a term that is no longer current
	// and may be truncated by the new leader, so it can never apply here.
	// Fail those waiters now with a not-leader redirect instead of leaving
	// them blocked until the caller's context expires. (A committed entry is
	// signaled earlier in this same drainReady pass, before publishStatus, so
	// this only reaches genuinely uncommitted waiters.)
	if role != raft.Leader && (len(g.waiters) > 0 || len(g.reads) > 0) {
		g.failWaiters(&ErrNotLeader{Leader: lead})
	}
}

// failWaiters resolves every pending proposal and read waiter with err and
// clears them. Runs on the event loop, the sole accessor of these maps; the
// resp channels are buffered so the sends never block.
func (g *group) failWaiters(err error) {
	for k, ch := range g.waiters {
		delete(g.waiters, k)
		ch <- err
	}
	for id, ch := range g.reads {
		delete(g.reads, id)
		ch <- err
	}
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
	return g.ProposeWithGuard(ctx, cmd, nil)
}

// ProposeWithGuard proposes a command with a guard that runs on the group's
// event loop immediately before the entry is appended, serialized with every
// other proposal. The sharded front door uses it to enforce the write freeze
// right up to the append: its front-door map check alone leaves a window
// where a request that passed before the freeze arrived could append after
// the migration's barrier and be missed by the final copy. Because the guard
// runs in loop order, every write either lands before the barrier (and is
// captured by the copy) or observes the frozen map and is rejected.
func (g *group) ProposeWithGuard(ctx context.Context, cmd *basalt.Command, guard func() error) error {
	p := &proposal{data: basalt.EncodeCommand(cmd), key: waiterKey{cmd.ClientID, cmd.Seq}, guard: guard, resp: make(chan error, 1)}
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

// adminReq drives a leader-only admin op (leadership transfer or membership
// change) on the group's event loop.
type adminReq struct {
	fn   func() error
	resp chan error
}

// runAdmin submits an admin closure to the loop; run() must poll adminc.
func (g *group) runAdmin(ctx context.Context, fn func() error) error {
	req := &adminReq{fn: fn, resp: make(chan error, 1)}
	select {
	case g.adminc <- req:
	case <-ctx.Done():
		return ctx.Err()
	case <-g.stopc:
		return errors.New("cluster: group stopped")
	}
	select {
	case err := <-req.resp:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-g.stopc:
		return errors.New("cluster: group stopped")
	}
}

// TransferLeader hands leadership to target (a voter in this group).
func (g *group) TransferLeader(ctx context.Context, target uint64) error {
	return g.runAdmin(ctx, func() error {
		if !g.raft.TransferLeader(target) {
			return &ErrNotLeader{Leader: g.raft.Lead()}
		}
		return nil
	})
}

// ConfChange proposes a single-server membership change on this group.
func (g *group) ConfChange(ctx context.Context, cc raft.ConfChange) error {
	return g.runAdmin(ctx, func() error { return g.raft.ProposeConfChange(cc) })
}

// NumVoters returns the group's current voting-member count, read on the
// event loop so it never races the raft node.
func (g *group) NumVoters(ctx context.Context) (int, error) {
	var n int
	err := g.runAdmin(ctx, func() error { n = g.raft.NumVoters(); return nil })
	return n, err
}

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
