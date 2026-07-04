// Package rafttest is a deterministic simulation harness for the raft core:
// a virtual clock and an in-memory network with seeded message loss,
// duplication, reordering, delay, and partitions. Everything is driven by a
// single seeded RNG — no wall clock, no goroutine scheduling — so any
// failure replays exactly from its seed.
package rafttest

import (
	"fmt"
	"path/filepath"
	"sort"

	"github.com/LovRanRan/basalt/internal/raft"
)

// Network options; zero values mean a perfect link.
type Options struct {
	Seed         uint64
	DropRate     float64 // per-message drop probability
	DupRate      float64 // per-message duplication probability
	MaxDelay     int     // extra ticks a message may sit in flight (0 = next tick)
	ElectionTick int     // per-node election timeout base (default 10)
	Dir          string  // when set, nodes persist here and support Restart
}

type inflight struct {
	deliverAt uint64
	seq       uint64 // tiebreak so ordering is total and deterministic
	msg       raft.Message
}

// Network owns the nodes and the message queue.
type Network struct {
	Nodes map[uint64]*raft.Node

	ids       []uint64
	opts      Options
	rng       func() uint64
	clock     uint64
	queue     []inflight
	seqCtr    uint64
	partition map[uint64]bool // isolated node ids
	appliedBy map[uint64][]raft.Entry
	store     map[uint64]*raft.Storage
	dir       string // when set, nodes persist here and can be restarted
}

// NewNetwork builds a cluster of ids wired through the simulated network.
// If opts.Dir is set, each node persists to opts.Dir and can be Restarted.
func NewNetwork(ids []uint64, opts Options) *Network {
	if opts.ElectionTick <= 0 {
		opts.ElectionTick = 10
	}
	net := &Network{
		Nodes:     map[uint64]*raft.Node{},
		ids:       append([]uint64(nil), ids...),
		opts:      opts,
		rng:       splitmix(opts.Seed),
		partition: map[uint64]bool{},
		appliedBy: map[uint64][]raft.Entry{},
		store:     map[uint64]*raft.Storage{},
		dir:       opts.Dir,
	}
	sort.Slice(net.ids, func(i, j int) bool { return net.ids[i] < net.ids[j] })
	for _, id := range net.ids {
		net.open(id)
	}
	return net
}

func (net *Network) cfg(id uint64) raft.Config {
	return raft.Config{
		ID: id, Peers: net.ids, ElectionTick: net.opts.ElectionTick,
		Rand: func(hi int) int { return int(net.rng() % uint64(hi)) },
	}
}

func (net *Network) open(id uint64) {
	if net.dir == "" {
		net.Nodes[id] = raft.NewNodeConfig(net.cfg(id))
		return
	}
	st, rec, err := raft.OpenStorage(filepath.Join(net.dir, fmt.Sprintf("node-%d", id)))
	if err != nil {
		panic(fmt.Sprintf("rafttest: open storage %d: %v", id, err))
	}
	net.store[id] = st
	// appliedBy is the durable "state machine": its length is the applied
	// index (entries are contiguous from index 1; no compaction here).
	net.Nodes[id] = raft.RestoreNode(net.cfg(id), rec, uint64(len(net.appliedBy[id])))
}

// Restart simulates a crash + reboot of one node: it drops the in-memory
// node and reopens it from its persisted storage. Only valid when the
// network was created with a Dir.
func (net *Network) Restart(id uint64) {
	if net.dir == "" {
		panic("rafttest: Restart requires Options.Dir")
	}
	_ = net.store[id].Close()
	// Drop in-flight messages TO the restarted node (a crash loses them).
	var kept []inflight
	for _, f := range net.queue {
		if f.msg.To != id {
			kept = append(kept, f)
		}
	}
	net.queue = kept
	net.open(id)
}

func (net *Network) prob(p float64) bool {
	if p <= 0 {
		return false
	}
	return float64(net.rng()%1_000_000)/1_000_000 < p
}

// Partition isolates the given nodes: messages to or from them are dropped
// until Heal.
func (net *Network) Partition(ids ...uint64) {
	for _, id := range ids {
		net.partition[id] = true
	}
}

func (net *Network) Heal() { net.partition = map[uint64]bool{} }

// Campaign starts an election on one node.
func (net *Network) Campaign(id uint64) { net.Nodes[id].Campaign() }

// Propose submits a command to a node, returning its NotLeader error if
// any.
func (net *Network) Propose(id uint64, data []byte) error {
	return net.Nodes[id].Propose(data)
}

// pump drains every node's Ready, persisting/applying inline (the harness
// is the storage layer) and enqueuing outbound messages with random delay.
func (net *Network) pump() {
	for _, id := range net.ids {
		n := net.Nodes[id]
		if !n.HasReady() {
			continue
		}
		rd := n.Ready()
		// Persist before send — the fsync-before-send rule. On restart
		// the node recovers exactly this persisted state.
		if st := net.store[id]; st != nil {
			if err := st.SaveReady(rd); err != nil {
				panic(fmt.Sprintf("rafttest: persist %d: %v", id, err))
			}
		}
		net.appliedBy[id] = append(net.appliedBy[id], rd.CommittedEntries...)
		for _, m := range rd.Messages {
			net.enqueue(m)
		}
		n.Advance(rd)
	}
}

func (net *Network) enqueue(m raft.Message) {
	if net.partition[m.From] || net.partition[m.To] {
		return
	}
	if net.prob(net.opts.DropRate) {
		return
	}
	copies := 1
	if net.prob(net.opts.DupRate) {
		copies = 2
	}
	for i := 0; i < copies; i++ {
		delay := uint64(1)
		if net.opts.MaxDelay > 0 {
			delay = 1 + net.rng()%uint64(net.opts.MaxDelay+1)
		}
		net.seqCtr++
		net.queue = append(net.queue, inflight{deliverAt: net.clock + delay, seq: net.seqCtr, msg: m})
	}
}

// deliverDue steps every message due at or before the current clock, in
// (deliverAt, seq) order so delivery is deterministic even when reordered.
func (net *Network) deliverDue() {
	sort.Slice(net.queue, func(i, j int) bool {
		if net.queue[i].deliverAt != net.queue[j].deliverAt {
			return net.queue[i].deliverAt < net.queue[j].deliverAt
		}
		return net.queue[i].seq < net.queue[j].seq
	})
	var rest []inflight
	for _, f := range net.queue {
		if f.deliverAt > net.clock {
			rest = append(rest, f)
			continue
		}
		if net.partition[f.msg.From] || net.partition[f.msg.To] {
			continue // partition dropped it in flight
		}
		if err := net.Nodes[f.msg.To].Step(f.msg); err != nil && err != raft.ErrNotLeader {
			panic(fmt.Sprintf("rafttest: step: %v", err))
		}
	}
	net.queue = rest
}

// Tick advances the virtual clock by one: ticks every node, delivers due
// messages, then pumps Ready. Repeats internally until Ready is quiet so a
// single Tick fully settles this instant's cascades.
func (net *Network) Tick() {
	net.clock++
	for _, id := range net.ids {
		net.Nodes[id].Tick()
	}
	net.settle()
}

// settle drives delivery + pump until no node has pending Ready and no
// message is due now.
func (net *Network) settle() {
	for i := 0; i < 10_000; i++ {
		net.deliverDue()
		net.pump()
		if !net.anyReady() && !net.anyDueNow() {
			return
		}
	}
	panic("rafttest: settle did not converge")
}

func (net *Network) anyReady() bool {
	for _, id := range net.ids {
		if net.Nodes[id].HasReady() {
			return true
		}
	}
	return false
}

func (net *Network) anyDueNow() bool {
	for _, f := range net.queue {
		if f.deliverAt <= net.clock && !net.partition[f.msg.From] && !net.partition[f.msg.To] {
			return true
		}
	}
	return false
}

// Run advances n ticks.
func (net *Network) Run(ticks int) {
	for i := 0; i < ticks; i++ {
		net.Tick()
	}
}

// Applied returns the commands (non-nil Data) applied by a node, in order.
func (net *Network) Applied(id uint64) [][]byte {
	var out [][]byte
	for _, e := range net.appliedBy[id] {
		if e.Data != nil {
			out = append(out, e.Data)
		}
	}
	return out
}

// Leader returns the single current leader id and true, or false if there
// is not exactly one leader at the highest term.
func (net *Network) Leader() (uint64, bool) {
	best := uint64(0)
	var leader uint64
	count := 0
	for _, id := range net.ids {
		n := net.Nodes[id]
		if n.Role() != raft.Leader {
			continue
		}
		if n.Term() > best {
			best, leader, count = n.Term(), id, 1
		} else if n.Term() == best {
			count++
		}
	}
	return leader, count == 1
}

// splitmix64 gives a deterministic, seed-reproducible stream.
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
