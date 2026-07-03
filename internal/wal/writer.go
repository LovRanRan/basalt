package wal

import (
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
)

// ErrRecordTooLarge rejects an Append whose payload exceeds MaxRecordSize.
// The writer stays usable: nothing was written.
var ErrRecordTooLarge = errors.New("wal: record exceeds MaxRecordSize")

// Writer appends records to the newest segment. Callers must serialize
// Append, Sync, and Close — the engine's write path is the single writer.
//
// A Writer never appends to a segment from a previous incarnation: OpenWriter
// truncates any torn tail off the newest existing segment, then starts a
// fresh one. Without that repair, a second restart would see the torn tail
// in a non-newest segment and misread a legitimate crash artifact as
// corruption.
//
// After any I/O error the writer is permanently failed: the stream may end
// in a partial frame, and appending past it would put acknowledged records
// where replay silently drops them. Every later call returns the first
// error; recovery is to discard the writer, OpenWriter again (repairing the
// torn frame), and Replay.
type Writer struct {
	opts   Options
	f      *os.File
	id     uint64
	size   int64
	buf    []byte
	err    error
	closed bool
}

func OpenWriter(opts Options) (*Writer, error) {
	if opts.SegmentSize <= 0 {
		opts.SegmentSize = 4 << 20
	}
	if opts.MaxRecordSize <= 0 {
		opts.MaxRecordSize = 64 << 20
	}
	if opts.MaxRecordSize > math.MaxUint32 {
		opts.MaxRecordSize = math.MaxUint32
	}
	if err := os.MkdirAll(opts.Dir, 0o755); err != nil {
		return nil, err
	}
	// Make the WAL directory's own dirent durable: fsyncing files inside a
	// directory does not persist the directory's entry in its parent, and
	// losing that entry loses every acked record with it.
	if err := syncDir(filepath.Dir(filepath.Clean(opts.Dir))); err != nil {
		return nil, err
	}
	ids, err := listSegments(opts.Dir)
	if err != nil {
		return nil, err
	}
	id := uint64(1)
	if len(ids) > 0 {
		last := ids[len(ids)-1]
		if err := repairTornTail(opts.Dir, last); err != nil {
			return nil, err
		}
		id = last + 1
	}
	w := &Writer{opts: opts, id: id}
	if err := w.openSegment(); err != nil {
		return nil, err
	}
	return w, nil
}

// repairTornTail truncates segment id at its first unparseable record and
// unconditionally fsyncs it. The sync matters even when the segment parses
// clean: a clean parse only proves page-cache contents, and the rotation
// invariant — a successor segment implies its predecessors are durable and
// whole — must hold before OpenWriter creates the fresh segment. Truncation
// cannot distinguish a torn write from media corruption at the same offset
// and discards either; the same trade LevelDB's recovery makes.
func repairTornTail(dir string, id uint64) error {
	f, err := os.OpenFile(filepath.Join(dir, segmentName(id)), os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	buf, err := io.ReadAll(f)
	if err != nil {
		return err
	}
	clean, _ := ScanRecords(buf, nil)
	if clean != len(buf) {
		if err := f.Truncate(int64(clean)); err != nil {
			return err
		}
	}
	return f.Sync()
}

func (w *Writer) openSegment() error {
	f, err := os.OpenFile(filepath.Join(w.opts.Dir, segmentName(w.id)), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	w.f, w.size = f, 0
	return syncDir(w.opts.Dir)
}

// fail poisons the writer with its first error.
func (w *Writer) fail(err error) error {
	if w.err == nil {
		w.err = fmt.Errorf("wal: writer failed, reopen to recover: %w", err)
	}
	return err
}

// Append writes one record — durably, under SyncEveryRecord — and returns
// the id of the segment the record landed in. Rotation happens after the
// write, so the returned id can trail a subsequent SegmentID call: a
// record's segment must be taken from this return value.
//
// On error the record's durability is indeterminate and the writer is
// permanently failed (except for ErrRecordTooLarge, which writes nothing).
func (w *Writer) Append(payload []byte) (uint64, error) {
	if w.err != nil {
		return 0, w.err
	}
	if w.closed {
		return 0, errors.New("wal: writer is closed")
	}
	if int64(len(payload)) > w.opts.MaxRecordSize {
		return 0, ErrRecordTooLarge
	}
	id := w.id
	w.buf = AppendRecord(w.buf[:0], payload)
	if _, err := w.f.Write(w.buf); err != nil {
		return 0, w.fail(err)
	}
	w.size += int64(len(w.buf))
	if w.opts.Sync == SyncEveryRecord {
		if err := w.f.Sync(); err != nil {
			return 0, w.fail(err)
		}
	}
	if w.size >= w.opts.SegmentSize {
		if err := w.rotate(); err != nil {
			return 0, w.fail(err)
		}
	}
	return id, nil
}

// rotate makes the finished segment durable before creating its successor.
// Together with open-time repair, this establishes the invariant replay
// relies on: the existence of segment N+1 proves segment N is complete,
// clean, and durable, so only the newest segment's tail may be torn.
func (w *Writer) rotate() error {
	if err := w.f.Sync(); err != nil {
		return err
	}
	if err := w.f.Close(); err != nil {
		return err
	}
	w.id++
	return w.openSegment()
}

// Sync makes all appended records durable; used with SyncManual batching.
func (w *Writer) Sync() error {
	if w.err != nil {
		return w.err
	}
	if w.closed {
		return errors.New("wal: writer is closed")
	}
	if err := w.f.Sync(); err != nil {
		return w.fail(err)
	}
	return nil
}

// SegmentID returns the id of the segment the next Append will write to.
func (w *Writer) SegmentID() uint64 { return w.id }

func (w *Writer) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true
	if w.err != nil {
		w.f.Close()
		return w.err
	}
	if err := w.f.Sync(); err != nil {
		w.f.Close()
		return w.fail(err)
	}
	return w.f.Close()
}

// DeleteSegmentsBefore removes segments with id < upto, whose contents the
// caller has made durable elsewhere (flushed to SSTables). The segment
// currently being written is never deleted.
func (w *Writer) DeleteSegmentsBefore(upto uint64) error {
	ids, err := listSegments(w.opts.Dir)
	if err != nil {
		return err
	}
	for _, id := range ids {
		if id < upto && id != w.id {
			if err := os.Remove(filepath.Join(w.opts.Dir, segmentName(id))); err != nil {
				return err
			}
			// Sync per removal, in ascending id order, so a crash
			// mid-deletion still leaves a contiguous id range —
			// Replay treats a gap as corruption.
			if err := syncDir(w.opts.Dir); err != nil {
				return err
			}
		}
	}
	return nil
}
