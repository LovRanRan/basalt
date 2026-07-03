package wal

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// Replay streams every committed record's payload to fn, across all
// segments in id order.
//
// Corruption semantics are honest about what is knowable. Within the newest
// segment, a bad record cannot be distinguished from the torn tail of a
// crash, so everything from the first bad byte onward is dropped silently —
// even records that were previously synced. Older segments were sealed
// durable by rotation or open-time repair before their successor existed,
// so damage there — including a gap in the segment id sequence — returns
// ErrCorrupt rather than papering over lost data. A missing directory
// replays as empty.
//
// The payload passed to fn aliases an internal buffer and is only valid for
// the duration of the call.
func Replay(dir string, fn func(payload []byte) error) error {
	ids, err := listSegments(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	for i, id := range ids {
		if i > 0 && id != ids[i-1]+1 {
			return fmt.Errorf("%w: missing segment %s", ErrCorrupt, segmentName(ids[i-1]+1))
		}
		buf, err := os.ReadFile(filepath.Join(dir, segmentName(id)))
		if err != nil {
			return err
		}
		clean, err := ScanRecords(buf, fn)
		if err != nil {
			return err
		}
		if clean != len(buf) {
			if i == len(ids)-1 {
				return nil
			}
			return fmt.Errorf("%w: segment %s", ErrCorrupt, segmentName(id))
		}
	}
	return nil
}
