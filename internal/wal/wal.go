// Package wal implements the write-ahead log: an append-only sequence of
// crc-framed records across segment files, replayed in id order on startup,
// with the torn tail of a crash cleanly ignored.
package wal

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
)

const segmentSuffix = ".wal"

// ErrCorrupt reports damage replay can prove is not a torn tail: a bad
// record or a missing segment strictly before the newest segment. Sealed
// segments are durable and whole by the rotation invariant, so damage there
// means acknowledged data was lost and replay refuses to paper over it.
// (Within the newest segment no such proof exists — see Replay.)
var ErrCorrupt = errors.New("wal: corrupt record before end of log")

// SyncPolicy controls when appends are made durable.
//
// Durability relies on os.File.Sync: fsync(2) on Linux, and on macOS
// F_FULLFSYNC (with an fsync fallback where the filesystem lacks it), so a
// returned Sync means data reached stable storage — at the cost of a full
// drive-cache flush per call on Darwin. SyncEveryRecord therefore bounds
// throughput at roughly one cache flush per append; prefer SyncManual with
// group commit when that matters.
type SyncPolicy int

const (
	// SyncEveryRecord fsyncs before every Append returns.
	SyncEveryRecord SyncPolicy = iota
	// SyncManual leaves fsync to explicit Sync calls: a crash may lose
	// records appended since the last Sync, but never reorders them.
	SyncManual
)

type Options struct {
	Dir string
	// SegmentSize is the rotation threshold in bytes; a segment may exceed
	// it by up to one record. Defaults to 4 MiB.
	SegmentSize int64
	// MaxRecordSize bounds a single payload; larger appends fail with
	// ErrRecordTooLarge. The bound keeps whole-segment reads during replay
	// legitimate: no segment exceeds SegmentSize + MaxRecordSize. Defaults
	// to 64 MiB, capped at the u32 length field.
	MaxRecordSize int64
	Sync          SyncPolicy
}

func segmentName(id uint64) string { return fmt.Sprintf("%06d%s", id, segmentSuffix) }

func parseSegmentName(name string) (uint64, bool) {
	s, ok := strings.CutSuffix(name, segmentSuffix)
	if !ok || s == "" {
		return 0, false
	}
	id, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, false
	}
	return id, true
}

// listSegments returns segment ids in ascending order.
func listSegments(dir string) ([]uint64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var ids []uint64
	for _, e := range entries {
		if id, ok := parseSegmentName(e.Name()); ok {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids, nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = d.Sync()
	if cerr := d.Close(); err == nil {
		err = cerr
	}
	return err
}
