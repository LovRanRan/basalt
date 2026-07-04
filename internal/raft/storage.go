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
	recCompact   byte = 4 // snapshot boundary: [index u64][term u64]; first record of a rewritten file
)

// OpenStorage opens or creates the state file at path and replays it into
// the recovered state: hard state, snapshot boundary, and the surviving
// entries (contiguous from SnapIndex+1).
func OpenStorage(path string) (*Storage, Recovered, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, Recovered{}, err
	}
	buf, err := os.ReadFile(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, Recovered{}, err
	}
	var rec Recovered
	// Replay: a torn final record is a crash artifact and ends replay.
	_, rerr := wal.ScanRecords(buf, func(payload []byte) error {
		return applyRecord(payload, &rec)
	})
	if rerr != nil {
		return nil, Recovered{}, rerr
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, Recovered{}, err
	}
	if _, err := f.Seek(0, 2); err != nil {
		_ = f.Close()
		return nil, Recovered{}, err
	}
	return &Storage{path: path, f: f, lastIndex: rec.SnapIndex + uint64(len(rec.Entries))}, rec, nil
}

func applyRecord(payload []byte, rec *Recovered) error {
	if len(payload) < 1 {
		return fmt.Errorf("raft: empty storage record")
	}
	body := payload[1:]
	switch payload[0] {
	case recHardState:
		if len(body) != 24 {
			return fmt.Errorf("raft: bad hardstate record")
		}
		rec.HardState.Term = binary.LittleEndian.Uint64(body[0:])
		rec.HardState.Vote = binary.LittleEndian.Uint64(body[8:])
		rec.HardState.Commit = binary.LittleEndian.Uint64(body[16:])
	case recCompact:
		if len(body) != 16 {
			return fmt.Errorf("raft: bad compact record")
		}
		idx := binary.LittleEndian.Uint64(body[0:])
		if len(rec.Entries) != 0 || rec.SnapIndex != 0 {
			return fmt.Errorf("raft: compact record not at file start")
		}
		rec.SnapIndex = idx
		rec.SnapTerm = binary.LittleEndian.Uint64(body[8:])
	case recEntries:
		decoded, err := decodeEntries(body)
		if err != nil {
			return err
		}
		for _, e := range decoded {
			// Appends may overwrite a conflicting suffix that a later
			// truncate would also have removed; index them directly,
			// relative to the snapshot boundary.
			if e.Index <= rec.SnapIndex {
				return fmt.Errorf("raft: entry %d below snapshot %d", e.Index, rec.SnapIndex)
			}
			pos := e.Index - rec.SnapIndex // 1-based within Entries
			switch {
			case pos <= uint64(len(rec.Entries)):
				rec.Entries[pos-1] = e
				rec.Entries = rec.Entries[:pos]
			case pos == uint64(len(rec.Entries))+1:
				rec.Entries = append(rec.Entries, e)
			default:
				return fmt.Errorf("raft: gap in storage at index %d (have through %d)", e.Index, rec.SnapIndex+uint64(len(rec.Entries)))
			}
		}
	case recTruncate:
		if len(body) != 8 {
			return fmt.Errorf("raft: bad truncate record")
		}
		from := binary.LittleEndian.Uint64(body)
		if from <= rec.SnapIndex {
			return fmt.Errorf("raft: truncate %d below snapshot %d", from, rec.SnapIndex)
		}
		if pos := from - rec.SnapIndex; pos >= 1 && int(pos-1) < len(rec.Entries) {
			rec.Entries = rec.Entries[:pos-1]
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

// Rewrite atomically replaces the whole file with a compacted image: the
// snapshot boundary, the hard state, and the surviving entries. This is
// how the raft log's disk footprint stays bounded — the caller invokes it
// after capturing a state-machine snapshot and CompactTo on the node. The
// swap is tmp-write, fsync, rename, dir-fsync; a crash at any point leaves
// either the old or the new complete file.
func (s *Storage) Rewrite(hs HardState, snapIndex, snapTerm uint64, ents []Entry) error {
	tmp := s.path + ".tmp"
	var buf []byte
	var body [16]byte
	binary.LittleEndian.PutUint64(body[0:], snapIndex)
	binary.LittleEndian.PutUint64(body[8:], snapTerm)
	buf = wal.AppendRecord(buf, append([]byte{recCompact}, body[:]...))
	var hsb [24]byte
	binary.LittleEndian.PutUint64(hsb[0:], hs.Term)
	binary.LittleEndian.PutUint64(hsb[8:], hs.Vote)
	binary.LittleEndian.PutUint64(hsb[16:], hs.Commit)
	buf = wal.AppendRecord(buf, append([]byte{recHardState}, hsb[:]...))
	if len(ents) > 0 {
		buf = wal.AppendRecord(buf, append([]byte{recEntries}, encodeEntries(ents)...))
	}
	tf, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := tf.Write(buf); err != nil {
		_ = tf.Close()
		return err
	}
	if err := tf.Sync(); err != nil {
		_ = tf.Close()
		return err
	}
	if err := tf.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return err
	}
	if err := syncParentDir(s.path); err != nil {
		return err
	}
	// Swap the append handle to the new file.
	old := s.f
	f, err := os.OpenFile(s.path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	_ = old.Close()
	s.f = f
	s.lastIndex = snapIndex + uint64(len(ents))
	return nil
}

func syncParentDir(path string) error {
	d, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	err = d.Sync()
	if cerr := d.Close(); err == nil {
		err = cerr
	}
	return err
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
