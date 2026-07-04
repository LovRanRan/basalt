package raft

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/LovRanRan/basalt/internal/wal"
)

// Storage persists a raft node's durable state: the HardState (term, vote,
// commit) and the log entries. It reuses the P1.3 record framing
// (crc32c-checked, torn-tail-tolerant) but implements its own record
// stream — raft needs suffix truncation on conflicting entries, which the
// append-only WAL Writer deliberately forbids.
//
// The file is a sequence of framed records, each a typed event: a HardState
// snapshot or a batch of appended entries. Replay folds them into the
// current state; a truncation event marks entries above an index as gone.
// fsync-before-send is the caller's responsibility via SaveReady + Sync.
type Storage struct {
	path      string
	f         *os.File
	lastIndex uint64 // highest entry index currently persisted
}

const (
	recHardState byte = 1
	recEntries   byte = 2
	recTruncate  byte = 3 // drop entries with index >= payload
)

// OpenStorage opens or creates the state file at path and replays it into
// hs and ents (entries in index order, starting at index 1).
func OpenStorage(path string) (s *Storage, hs HardState, ents []Entry, err error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, HardState{}, nil, err
	}
	buf, err := os.ReadFile(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, HardState{}, nil, err
	}
	// Replay: a torn final record is a crash artifact and ends replay.
	_, rerr := wal.ScanRecords(buf, func(payload []byte) error {
		return applyRecord(payload, &hs, &ents)
	})
	if rerr != nil {
		return nil, HardState{}, nil, rerr
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, HardState{}, nil, err
	}
	if _, err := f.Seek(0, 2); err != nil {
		_ = f.Close()
		return nil, HardState{}, nil, err
	}
	return &Storage{path: path, f: f, lastIndex: uint64(len(ents))}, hs, ents, nil
}

func applyRecord(payload []byte, hs *HardState, ents *[]Entry) error {
	if len(payload) < 1 {
		return fmt.Errorf("raft: empty storage record")
	}
	body := payload[1:]
	switch payload[0] {
	case recHardState:
		if len(body) != 24 {
			return fmt.Errorf("raft: bad hardstate record")
		}
		hs.Term = binary.LittleEndian.Uint64(body[0:])
		hs.Vote = binary.LittleEndian.Uint64(body[8:])
		hs.Commit = binary.LittleEndian.Uint64(body[16:])
	case recEntries:
		decoded, err := decodeEntries(body)
		if err != nil {
			return err
		}
		for _, e := range decoded {
			// Appends may overwrite a conflicting suffix that a later
			// truncate would also have removed; index them directly.
			if e.Index == 0 {
				return fmt.Errorf("raft: zero index in storage")
			}
			if int(e.Index) <= len(*ents) {
				(*ents)[e.Index-1] = e
				*ents = (*ents)[:e.Index]
			} else if e.Index == uint64(len(*ents))+1 {
				*ents = append(*ents, e)
			} else {
				return fmt.Errorf("raft: gap in storage at index %d (have %d)", e.Index, len(*ents))
			}
		}
	case recTruncate:
		if len(body) != 8 {
			return fmt.Errorf("raft: bad truncate record")
		}
		from := binary.LittleEndian.Uint64(body)
		if from >= 1 && int(from-1) < len(*ents) {
			*ents = (*ents)[:from-1]
		}
	default:
		return fmt.Errorf("raft: unknown storage record %d", payload[0])
	}
	return nil
}

func encodeEntries(ents []Entry) []byte {
	buf := binary.LittleEndian.AppendUint32(nil, uint32(len(ents)))
	for _, e := range ents {
		buf = binary.LittleEndian.AppendUint64(buf, e.Index)
		buf = binary.LittleEndian.AppendUint64(buf, e.Term)
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(e.Data)))
		buf = append(buf, e.Data...)
	}
	return buf
}

func decodeEntries(body []byte) ([]Entry, error) {
	if len(body) < 4 {
		return nil, fmt.Errorf("raft: short entries record")
	}
	n := binary.LittleEndian.Uint32(body)
	body = body[4:]
	ents := make([]Entry, 0, n)
	for i := uint32(0); i < n; i++ {
		if len(body) < 20 {
			return nil, fmt.Errorf("raft: truncated entry header")
		}
		e := Entry{
			Index: binary.LittleEndian.Uint64(body[0:]),
			Term:  binary.LittleEndian.Uint64(body[8:]),
		}
		dl := binary.LittleEndian.Uint32(body[16:])
		body = body[20:]
		if uint64(dl) > uint64(len(body)) {
			return nil, fmt.Errorf("raft: entry data overruns record")
		}
		if dl > 0 {
			e.Data = append([]byte(nil), body[:dl]...)
		}
		body = body[dl:]
		ents = append(ents, e)
	}
	if len(body) != 0 {
		return nil, fmt.Errorf("raft: trailing bytes in entries record")
	}
	return ents, nil
}

func (s *Storage) appendRecord(tag byte, body []byte) error {
	payload := append([]byte{tag}, body...)
	rec := wal.AppendRecord(nil, payload)
	_, err := s.f.Write(rec)
	return err
}

// SaveHardState appends a HardState record (not yet durable until Sync).
func (s *Storage) SaveHardState(hs HardState) error {
	var body [24]byte
	binary.LittleEndian.PutUint64(body[0:], hs.Term)
	binary.LittleEndian.PutUint64(body[8:], hs.Vote)
	binary.LittleEndian.PutUint64(body[16:], hs.Commit)
	return s.appendRecord(recHardState, body[:])
}

// TruncateFrom records that entries at index >= from are gone (a
// conflicting-suffix truncation). It must be logged before the replacement
// entries so replay sees the truncation first.
func (s *Storage) TruncateFrom(from uint64) error {
	var body [8]byte
	binary.LittleEndian.PutUint64(body[:], from)
	return s.appendRecord(recTruncate, body[:])
}

// AppendEntries persists entries, logging a truncation first when they
// overwrite a conflicting suffix (first index <= what is already stored).
func (s *Storage) AppendEntries(ents []Entry) error {
	if len(ents) == 0 {
		return nil
	}
	if ents[0].Index <= s.lastIndex {
		if err := s.TruncateFrom(ents[0].Index); err != nil {
			return err
		}
	}
	if err := s.appendRecord(recEntries, encodeEntries(ents)); err != nil {
		return err
	}
	s.lastIndex = ents[len(ents)-1].Index
	return nil
}

// SaveReady persists a Ready's durable parts in the required order —
// truncation, entries, hard state — then fsyncs. The caller invokes this
// BEFORE sending the Ready's messages, satisfying raft's persist-before-send
// rule; then it applies CommittedEntries and calls the node's Advance.
func (s *Storage) SaveReady(rd Ready) error {
	if err := s.AppendEntries(rd.Entries); err != nil {
		return err
	}
	if rd.HardState != nil {
		if err := s.SaveHardState(*rd.HardState); err != nil {
			return err
		}
	}
	if len(rd.Entries) > 0 || rd.HardState != nil {
		return s.Sync()
	}
	return nil
}

// Sync makes every appended record durable. It must be called before any
// message that depends on the persisted state leaves the process.
func (s *Storage) Sync() error { return s.f.Sync() }

func (s *Storage) Close() error {
	err := s.f.Sync()
	if cerr := s.f.Close(); err == nil {
		err = cerr
	}
	return err
}
