// Package manifest tracks the live-file state of the engine crash-safely: a
// log of VersionEdits (reusing the WAL record framing) named by a CURRENT
// pointer file, replayed at open into an immutable Version of live tables
// per level. Rotation writes a full snapshot to a fresh manifest and swaps
// CURRENT via atomic rename.
package manifest

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/LovRanRan/basalt/internal/base"
)

// NumLevels is the depth of the LSM tree.
const NumLevels = 7

var ErrCorrupt = errors.New("manifest: corrupt")

func corruptf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrCorrupt, fmt.Sprintf(format, args...))
}

// FileMeta describes one live table.
type FileMeta struct {
	Level    int
	FileNum  uint64
	Size     uint64
	Smallest []byte // encoded internal keys
	Largest  []byte
}

type DeletedFile struct {
	Level   int
	FileNum uint64
}

// VersionEdit is one atomic state change. The counters are stamped into
// every logged record by the VersionSet so recovery can read them off the
// last edit.
type VersionEdit struct {
	HasLogNumber   bool
	LogNumber      uint64 // wal segments below this id are obsolete
	HasNextFileNum bool
	NextFileNum    uint64
	HasLastSeq     bool
	LastSeq        uint64
	AddFiles       []FileMeta
	DeleteFiles    []DeletedFile
}

func (e *VersionEdit) SetLogNumber(n uint64)   { e.HasLogNumber, e.LogNumber = true, n }
func (e *VersionEdit) SetNextFileNum(n uint64) { e.HasNextFileNum, e.NextFileNum = true, n }
func (e *VersionEdit) SetLastSeq(n uint64)     { e.HasLastSeq, e.LastSeq = true, n }

const (
	tagLogNumber   = 1
	tagNextFileNum = 2
	tagLastSeq     = 3
	tagAddFile     = 4
	tagDeleteFile  = 5
)

func (e *VersionEdit) encode(dst []byte) []byte {
	if e.HasLogNumber {
		dst = binary.AppendUvarint(dst, tagLogNumber)
		dst = binary.AppendUvarint(dst, e.LogNumber)
	}
	if e.HasNextFileNum {
		dst = binary.AppendUvarint(dst, tagNextFileNum)
		dst = binary.AppendUvarint(dst, e.NextFileNum)
	}
	if e.HasLastSeq {
		dst = binary.AppendUvarint(dst, tagLastSeq)
		dst = binary.AppendUvarint(dst, e.LastSeq)
	}
	for _, f := range e.AddFiles {
		dst = binary.AppendUvarint(dst, tagAddFile)
		dst = binary.AppendUvarint(dst, uint64(f.Level))
		dst = binary.AppendUvarint(dst, f.FileNum)
		dst = binary.AppendUvarint(dst, f.Size)
		dst = base.AppendLengthPrefixed(dst, f.Smallest)
		dst = base.AppendLengthPrefixed(dst, f.Largest)
	}
	for _, d := range e.DeleteFiles {
		dst = binary.AppendUvarint(dst, tagDeleteFile)
		dst = binary.AppendUvarint(dst, uint64(d.Level))
		dst = binary.AppendUvarint(dst, d.FileNum)
	}
	return dst
}

func decodeUvarint(buf []byte, what string) (uint64, []byte, error) {
	v, n := binary.Uvarint(buf)
	if n <= 0 {
		return 0, nil, corruptf("bad %s", what)
	}
	return v, buf[n:], nil
}

func decodeLevel(buf []byte) (int, []byte, error) {
	l, rest, err := decodeUvarint(buf, "level")
	if err != nil {
		return 0, nil, err
	}
	if l >= NumLevels {
		return 0, nil, corruptf("level %d out of range", l)
	}
	return int(l), rest, nil
}

// decodeEdit parses one record. Decoded key slices are copied: the record
// buffer is transient.
func decodeEdit(payload []byte) (VersionEdit, error) {
	var e VersionEdit
	rest := payload
	for len(rest) > 0 {
		tag, r, err := decodeUvarint(rest, "tag")
		if err != nil {
			return VersionEdit{}, err
		}
		rest = r
		switch tag {
		case tagLogNumber:
			e.LogNumber, rest, err = decodeUvarint(rest, "log number")
			e.HasLogNumber = true
		case tagNextFileNum:
			e.NextFileNum, rest, err = decodeUvarint(rest, "next file number")
			e.HasNextFileNum = true
		case tagLastSeq:
			e.LastSeq, rest, err = decodeUvarint(rest, "last seq")
			e.HasLastSeq = true
		case tagAddFile:
			var f FileMeta
			if f.Level, rest, err = decodeLevel(rest); err != nil {
				return VersionEdit{}, err
			}
			if f.FileNum, rest, err = decodeUvarint(rest, "file number"); err != nil {
				return VersionEdit{}, err
			}
			if f.Size, rest, err = decodeUvarint(rest, "file size"); err != nil {
				return VersionEdit{}, err
			}
			var s, l []byte
			if s, rest, err = base.DecodeLengthPrefixed(rest); err != nil {
				return VersionEdit{}, corruptf("add-file smallest: %v", err)
			}
			if l, rest, err = base.DecodeLengthPrefixed(rest); err != nil {
				return VersionEdit{}, corruptf("add-file largest: %v", err)
			}
			if len(s) < base.TrailerLen || len(l) < base.TrailerLen {
				return VersionEdit{}, corruptf("add-file bounds shorter than internal-key trailer")
			}
			f.Smallest = append([]byte(nil), s...)
			f.Largest = append([]byte(nil), l...)
			e.AddFiles = append(e.AddFiles, f)
		case tagDeleteFile:
			var d DeletedFile
			if d.Level, rest, err = decodeLevel(rest); err != nil {
				return VersionEdit{}, err
			}
			if d.FileNum, rest, err = decodeUvarint(rest, "file number"); err != nil {
				return VersionEdit{}, err
			}
			e.DeleteFiles = append(e.DeleteFiles, d)
		default:
			return VersionEdit{}, corruptf("unknown tag %d", tag)
		}
		if err != nil {
			return VersionEdit{}, err
		}
	}
	return e, nil
}
