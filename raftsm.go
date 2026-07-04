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

// AppliedTerm returns the term of the applied entry (snapshot metadata for
// P3.7).
func (sm *StateMachine) AppliedTerm() uint64 { return sm.term }

// Apply folds committed entries into the engine, in order. Each entry
// becomes one atomic engine batch: the user ops (unless the command is a
// duplicate — same client, non-increasing seq — in which case they are
// skipped), the session update, and the applied-index record. Duplicate or
// empty entries still advance the applied index durably.
func (sm *StateMachine) Apply(entries []raft.Entry) error {
	for _, e := range entries {
		if e.Index != sm.applied+1 {
			return fmt.Errorf("basalt: apply gap: entry %d after applied %d", e.Index, sm.applied)
		}
		var eb wal.Batch
		var sessClient, sessSeq uint64
		haveSess := false
		if e.Data != nil {
			cmd, err := DecodeCommand(e.Data)
			if err != nil {
				return fmt.Errorf("basalt: entry %d: %w", e.Index, err)
			}
			if cmd.Seq > sm.sessions[cmd.ClientID] {
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
			return err
		}
		if haveSess {
			sm.sessions[sessClient] = sessSeq
		}
		sm.applied = e.Index
		sm.term = e.Term
	}
	return nil
}
