package basalt

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/LovRanRan/basalt/internal/raft"
	"github.com/LovRanRan/basalt/internal/wal"
)

// Reserved keys the state machine maintains inside the engine. They ride in
// the SAME atomic batch as the user ops they describe, so the engine's
// recovered state always carries an applied index exactly consistent with
// its data — recovery is an exact prefix replay of the raft log from that
// index, never an idempotent re-apply.
var (
	appliedKey    = append(append([]byte(nil), reservedPrefix...), []byte("applied")...)
	sessionPrefix = append(append([]byte(nil), reservedPrefix...), []byte("session/")...)
)

// NoDedupClient is the high bit of ClientID, reserved as an ephemeral
// "no-dedup" namespace: commands whose ClientID has this bit set are always
// applied and record no dedup session. It is for idempotent bulk writers such
// as shard migration, where exactly-once is unnecessary (re-applying a key
// writes the same value) and a persisted session would be harmful — an
// ephemeral writer either collides with a stale session after its id counter
// resets, or, if it uses a fresh id per run, leaks sessions without bound.
// The low 63 bits still distinguish writers, so proposal waiter keys stay
// unique. Ordinary clients use small operator-assigned ids (bit clear).
const NoDedupClient = uint64(1) << 63

// Command is one replicated client request: a batch of ops stamped with the
// client's identity and per-client sequence number for exactly-once applies
// across leader changes and retries.
type Command struct {
	ClientID uint64
	Seq      uint64
	Batch    Batch
}

// EncodeCommand serializes a command for proposing through raft.
func EncodeCommand(c *Command) []byte {
	buf := binary.LittleEndian.AppendUint64(nil, c.ClientID)
	buf = binary.LittleEndian.AppendUint64(buf, c.Seq)
	return c.Batch.inner.Encode(buf)
}

// DecodeCommand parses a proposed command; the decoded ops alias data.
func DecodeCommand(data []byte) (Command, error) {
	if len(data) < 16 {
		return Command{}, errors.New("basalt: short command")
	}
	c := Command{
		ClientID: binary.LittleEndian.Uint64(data[0:]),
		Seq:      binary.LittleEndian.Uint64(data[8:]),
	}
	b, err := wal.DecodeBatch(data[16:])
	if err != nil {
		return Command{}, err
	}
	c.Batch.inner = b
	return c, nil
}

// StateMachine applies committed raft entries to the engine. Not safe for
// concurrent use: one apply loop owns it.
type StateMachine struct {
	db       *DB
	applied  uint64            // raft index applied through
	term     uint64            // term of that entry
	sessions map[uint64]uint64 // clientID -> highest applied client seq
}

// NewStateMachine binds the engine and recovers the applied index and the
// dedup session table from the reserved key range.
func NewStateMachine(db *DB) (*StateMachine, error) {
	sm := &StateMachine{db: db, sessions: map[uint64]uint64{}}
	v, err := db.get(appliedKey)
	switch {
	case errors.Is(err, ErrNotFound):
	case err != nil:
		return nil, err
	case len(v) != 16:
		return nil, fmt.Errorf("basalt: corrupt applied record (%d bytes)", len(v))
	default:
		sm.applied = binary.LittleEndian.Uint64(v[0:])
		sm.term = binary.LittleEndian.Uint64(v[8:])
	}
	end := append(append([]byte(nil), sessionPrefix...), 0xff)
	it := sm.db.scanAll(sessionPrefix, end)
	for ; it.Valid(); it.Next() {
		k, val := it.Key(), it.Value()
		if len(k) != len(sessionPrefix)+8 || len(val) != 8 {
			it.Close()
			return nil, fmt.Errorf("basalt: corrupt session record %q", k)
		}
		sm.sessions[binary.BigEndian.Uint64(k[len(sessionPrefix):])] = binary.LittleEndian.Uint64(val)
	}
	err = it.Error()
	it.Close()
	if err != nil {
		return nil, err
	}
	return sm, nil
}

// AppliedIndex returns the raft index the engine has durably applied
// through — the value RestoreNode needs.
func (sm *StateMachine) AppliedIndex() uint64 { return sm.applied }

// Snapshot captures the state machine at its current applied index as an
// engine checkpoint at dst, returning the raft (index, term) the snapshot
// covers. Checkpoint drains and flushes first, so after this returns the
// LIVE engine's durable state also covers the returned index — which is
// what makes it safe to compact the raft log to it: a crash right after
// compaction still recovers an engine at or beyond the snapshot boundary.
func (sm *StateMachine) Snapshot(dst string) (index, term uint64, err error) {
	if err := sm.db.Checkpoint(dst); err != nil {
		return 0, 0, err
	}
	return sm.applied, sm.term, nil
}

// ReadSnapshotMeta opens a snapshot directory (a checkpoint) read-only and
// returns the raft index/term it covers — what an InstallSnapshot receiver
// (P3.8) verifies before swapping it in.
func ReadSnapshotMeta(dir string) (index, term uint64, err error) {
	db, err := Open(dir, Options{DisableWAL: true})
	if err != nil {
		return 0, 0, err
	}
	defer func() { _ = db.Close() }()
	sm, err := NewStateMachine(db)
	if err != nil {
		return 0, 0, err
	}
	return sm.applied, sm.term, nil
}

// AppliedTerm returns the term of the applied entry (snapshot metadata for
// P3.7).
func (sm *StateMachine) AppliedTerm() uint64 { return sm.term }

// Applied reports one command that Apply committed, so the node can signal
// its proposer.
type Applied struct {
	ClientID uint64
	Seq      uint64
	Index    uint64
}

// Apply folds committed entries into the engine, in order. Each entry
// becomes one atomic engine batch: the user ops (unless the command is a
// duplicate — same client, non-increasing seq — in which case they are
// skipped), the session update, and the applied-index record. Duplicate or
// empty entries still advance the applied index durably. It returns the
// (clientID, seq) of every command entry applied, in order.
func (sm *StateMachine) Apply(entries []raft.Entry) ([]Applied, error) {
	var out []Applied
	for _, e := range entries {
		if e.Index != sm.applied+1 {
			return nil, fmt.Errorf("basalt: apply gap: entry %d after applied %d", e.Index, sm.applied)
		}
		var eb wal.Batch
		var sessClient, sessSeq uint64
		var cmdClient, cmdSeq uint64
		haveCmd := false
		haveSess := false
		// Configuration-change entries mutate raft membership, not the
		// engine: advance the applied index but decode no command.
		if e.Data != nil && e.Type != raft.EntryConfChange {
			cmd, err := DecodeCommand(e.Data)
			if err != nil {
				return nil, fmt.Errorf("basalt: entry %d: %w", e.Index, err)
			}
			cmdClient, cmdSeq, haveCmd = cmd.ClientID, cmd.Seq, true
			switch {
			case cmd.ClientID&NoDedupClient != 0:
				// Ephemeral no-dedup namespace: always apply, no session.
				eb.Ops = append(eb.Ops, cmd.Batch.inner.Ops...)
			case cmd.Seq > sm.sessions[cmd.ClientID]:
				eb.Ops = append(eb.Ops, cmd.Batch.inner.Ops...)
				sk := append(append([]byte(nil), sessionPrefix...), make([]byte, 8)...)
				binary.BigEndian.PutUint64(sk[len(sessionPrefix):], cmd.ClientID)
				eb.Put(sk, binary.LittleEndian.AppendUint64(nil, cmd.Seq))
				sessClient, sessSeq, haveSess = cmd.ClientID, cmd.Seq, true
			}
		}
		av := binary.LittleEndian.AppendUint64(nil, e.Index)
		av = binary.LittleEndian.AppendUint64(av, e.Term)
		eb.Put(appliedKey, av)
		if err := sm.db.applyRaft(&eb); err != nil {
			return nil, err
		}
		if haveSess {
			sm.sessions[sessClient] = sessSeq
		}
		if haveCmd {
			out = append(out, Applied{ClientID: cmdClient, Seq: cmdSeq, Index: e.Index})
		}
		sm.applied = e.Index
		sm.term = e.Term
	}
	return out, nil
}
