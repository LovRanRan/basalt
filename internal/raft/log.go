package raft

import "fmt"

// raftLog is the in-memory log. entries[i] holds index offset+i; snapIndex
// and snapTerm describe the compacted prefix (the last entry folded into a
// snapshot), and offset == snapIndex+1. Index 0 is the implicit empty
// prefix with term 0.
//
// committed is the highest index known safely replicated; applied is the
// highest handed to the state machine. snapIndex <= applied <= committed
// <= lastIndex always; violations are logic errors and panic.
type raftLog struct {
	entries   []Entry
	offset    uint64
	snapIndex uint64
	snapTerm  uint64
	committed uint64
	applied   uint64
}

func newLog() *raftLog {
	return &raftLog{offset: 1}
}

func (l *raftLog) firstIndex() uint64 { return l.offset }

// compactTo folds the prefix through index into a snapshot: the entries are
// dropped and only (index, term) survives so consistency checks against the
// boundary still work. Compaction never passes the applied index — those
// entries could still be needed to replay a crashed state machine.
func (l *raftLog) compactTo(index uint64) {
	if index <= l.snapIndex {
		return
	}
	if index > l.applied {
		panic(fmt.Sprintf("raft: compact to %d beyond applied %d", index, l.applied))
	}
	t, ok := l.term(index)
	if !ok {
		panic(fmt.Sprintf("raft: compact to unknown index %d", index))
	}
	l.entries = append([]Entry(nil), l.entries[index-l.offset+1:]...)
	l.offset = index + 1
	l.snapIndex, l.snapTerm = index, t
}

func (l *raftLog) lastIndex() uint64 { return l.offset + uint64(len(l.entries)) - 1 }

func (l *raftLog) lastTerm() uint64 {
	t, ok := l.term(l.lastIndex())
	if !ok {
		panic("raft: last term unavailable")
	}
	return t
}

// term returns the term of index i; ok=false when i is outside the log
// (compacted away or past the tail). The snapshot boundary itself keeps
// its term so AppendEntries consistency checks against it succeed.
func (l *raftLog) term(i uint64) (uint64, bool) {
	if i == l.snapIndex {
		return l.snapTerm, true
	}
	if i < l.offset || i > l.lastIndex() {
		return 0, false
	}
	return l.entries[i-l.offset].Term, true
}

// append adds entries at the tail; the first entry must directly follow
// lastIndex — conflict resolution is maybeAppend's job.
func (l *raftLog) append(ents ...Entry) {
	if len(ents) == 0 {
		return
	}
	if ents[0].Index != l.lastIndex()+1 {
		panic(fmt.Sprintf("raft: append at %d, want %d", ents[0].Index, l.lastIndex()+1))
	}
	l.entries = append(l.entries, ents...)
}

// maybeAppend implements the AppendEntries consistency check: it accepts
// ents when the log contains prevIndex with prevTerm, truncating any
// conflicting suffix first, and advances commit. ok=false rejects.
func (l *raftLog) maybeAppend(prevIndex, prevTerm, commit uint64, ents []Entry) (lastNew uint64, ok bool) {
	t, have := l.term(prevIndex)
	if !have || t != prevTerm {
		return 0, false
	}
	lastNew = prevIndex + uint64(len(ents))
	for i, e := range ents {
		et, have := l.term(e.Index)
		if !have {
			l.append(ents[i:]...)
			break
		}
		if et != e.Term {
			l.truncateFrom(e.Index)
			l.append(ents[i:]...)
			break
		}
	}
	l.commitTo(min(commit, lastNew))
	return lastNew, true
}

// truncateFrom drops every entry at index >= i. Truncating committed
// entries would un-agree agreed state; that is a protocol violation and
// panics.
func (l *raftLog) truncateFrom(i uint64) {
	if i <= l.committed {
		panic(fmt.Sprintf("raft: truncate at %d below committed %d", i, l.committed))
	}
	if i < l.offset {
		panic(fmt.Sprintf("raft: truncate at %d below first index %d", i, l.offset))
	}
	l.entries = l.entries[:i-l.offset]
}

// slice returns entries in [lo, hi); bounds must be inside the log.
func (l *raftLog) slice(lo, hi uint64) []Entry {
	if lo < l.offset || hi > l.lastIndex()+1 || lo > hi {
		panic(fmt.Sprintf("raft: slice [%d,%d) outside [%d,%d]", lo, hi, l.offset, l.lastIndex()))
	}
	if lo == hi {
		return nil
	}
	return l.entries[lo-l.offset : hi-l.offset]
}

// commitTo advances the commit index; it never regresses and never passes
// the last index.
func (l *raftLog) commitTo(i uint64) {
	if i <= l.committed {
		return
	}
	if i > l.lastIndex() {
		panic(fmt.Sprintf("raft: commit %d beyond last index %d", i, l.lastIndex()))
	}
	l.committed = i
}

func (l *raftLog) appliedTo(i uint64) {
	if i < l.applied || i > l.committed {
		panic(fmt.Sprintf("raft: applied %d outside (%d, %d]", i, l.applied, l.committed))
	}
	l.applied = i
}

// isUpToDate reports whether a candidate with the given last entry is at
// least as current as this log — the vote-granting rule.
func (l *raftLog) isUpToDate(lastIndex, lastTerm uint64) bool {
	if lastTerm != l.lastTerm() {
		return lastTerm > l.lastTerm()
	}
	return lastIndex >= l.lastIndex()
}
