package basalt

import (
	"bufio"
	"bytes"
	"fmt"
	"os"

	"github.com/LovRanRan/basalt/internal/base"
	"github.com/LovRanRan/basalt/internal/manifest"
	"github.com/LovRanRan/basalt/internal/sstable"
)

// compaction is one unit of work: merge inputs[0] (source level) with
// inputs[1] (their overlaps in the output level) into fresh output-level
// tables. below holds the file bounds of every level deeper than the
// output — the tombstone-GC check. Input handles are pinned (ref'd) at pick
// time and released when the compaction finishes.
type compaction struct {
	level, output int
	inputs        [2][]*tableHandle
	inMeta        [2][]manifest.FileMeta
	below         [][]manifest.FileMeta
}

// maybeScheduleCompactionLocked starts a background compaction when a level
// is over its trigger. One compaction runs at a time; completion reschedules
// so cascades drain. Callers hold mu.
func (db *DB) maybeScheduleCompactionLocked() {
	if db.compacting || db.closing || db.bgErr != nil {
		return
	}
	c := db.pickCompactionLocked()
	if c == nil {
		return
	}
	for _, side := range c.inputs {
		for _, h := range side {
			h.ref()
		}
	}
	db.compacting = true
	go db.compact(c)
}

// pickCompactionLocked scores levels — L0 by file count, deeper levels by
// bytes against a 10x-per-level budget — and builds the input set for the
// highest score at or above 1.
func (db *DB) pickCompactionLocked() *compaction {
	v := db.vs.Current()
	bestLevel := -1
	bestScore := 1.0
	if score := float64(len(v.Files[0])) / float64(db.opts.L0CompactThreshold); score >= bestScore {
		bestLevel, bestScore = 0, score
	}
	for lvl := 1; lvl < manifest.NumLevels-1; lvl++ {
		var total uint64
		for _, m := range v.Files[lvl] {
			total += m.Size
		}
		if score := float64(total) / float64(db.maxBytes(lvl)); score >= bestScore {
			bestLevel, bestScore = lvl, score
		}
	}
	if bestLevel < 0 {
		return nil
	}

	c := &compaction{level: bestLevel, output: bestLevel + 1}
	if bestLevel == 0 {
		// L0 files overlap arbitrarily; take them all.
		c.inMeta[0] = append(c.inMeta[0], v.Files[0]...)
	} else {
		// Rotate through the level so every file eventually compacts.
		files := v.Files[bestLevel]
		idx := 0
		if ptr := db.compactPtr[bestLevel]; ptr != nil {
			for idx < len(files) && base.Compare(files[idx].Smallest, ptr) <= 0 {
				idx++
			}
			if idx == len(files) {
				idx = 0
			}
		}
		c.inMeta[0] = []manifest.FileMeta{files[idx]}
		db.compactPtr[bestLevel] = append([]byte(nil), files[idx].Smallest...)
	}
	minUK, maxUK := userKeyRange(c.inMeta[0])
	c.inMeta[1] = overlapping(v.Files[c.output], minUK, maxUK)

	for side, metas := range c.inMeta {
		for _, m := range metas {
			h := db.tables[m.FileNum]
			if h == nil {
				db.bgErr = fmt.Errorf("basalt: compaction input %06d has no open handle", m.FileNum)
				return nil
			}
			c.inputs[side] = append(c.inputs[side], h)
		}
	}
	for lvl := c.output + 1; lvl < manifest.NumLevels; lvl++ {
		c.below = append(c.below, v.Files[lvl])
	}
	return c
}

func (db *DB) maxBytes(level int) int64 {
	max := db.opts.LevelBytesBase
	for i := 1; i < level; i++ {
		max *= 10
	}
	return max
}

func userKeyRange(metas []manifest.FileMeta) (minUK, maxUK []byte) {
	for _, m := range metas {
		s, l := base.UserKey(m.Smallest), base.UserKey(m.Largest)
		if minUK == nil || bytes.Compare(s, minUK) < 0 {
			minUK = s
		}
		if maxUK == nil || bytes.Compare(l, maxUK) > 0 {
			maxUK = l
		}
	}
	return minUK, maxUK
}

// overlapping returns the files whose user-key range intersects [minUK,
// maxUK].
func overlapping(metas []manifest.FileMeta, minUK, maxUK []byte) []manifest.FileMeta {
	var out []manifest.FileMeta
	for _, m := range metas {
		if bytes.Compare(base.UserKey(m.Largest), minUK) < 0 || bytes.Compare(base.UserKey(m.Smallest), maxUK) > 0 {
			continue
		}
		out = append(out, m)
	}
	return out
}

// keyMayExistBelow reports whether any level deeper than the compaction's
// output could hold uk; a tombstone may only be dropped when nothing below
// could resurrect the key.
func (c *compaction) keyMayExistBelow(uk []byte) bool {
	for _, level := range c.below {
		for _, m := range level {
			if bytes.Compare(uk, base.UserKey(m.Smallest)) >= 0 && bytes.Compare(uk, base.UserKey(m.Largest)) <= 0 {
				return true
			}
		}
	}
	return false
}

// compact merges the inputs into output-level tables, keeping only the
// newest version of each user key (older versions are shadowed) and
// dropping tombstones that nothing deeper can resurrect, then commits the
// swap as one atomic manifest edit. On failure the engine poisons itself;
// orphan outputs are collected at the next open.
func (db *DB) compact(c *compaction) {
	var clearPending func()
	finish := func(err error) {
		db.mu.Lock()
		if err != nil && db.bgErr == nil {
			db.bgErr = fmt.Errorf("basalt: compaction: %w", err)
		}
		clearPending()
		for _, side := range c.inputs {
			for _, h := range side {
				h.unref()
			}
		}
		db.compacting = false
		if err == nil {
			db.maybeScheduleCompactionLocked()
		}
		db.cond.Broadcast()
		db.mu.Unlock()
	}

	var children []internalIterator
	for _, side := range c.inputs {
		for _, h := range side {
			children = append(children, h.r.NewIterator())
		}
	}
	m := newMergingIter(children)

	var (
		outputs []manifest.FileMeta
		outNums []uint64
		w       *sstable.Writer
		bw      *bufio.Writer
		f       *os.File
		num     uint64
		lastUK  []byte
		haveUK  bool
	)
	clearPending = func() {
		for _, n := range outNums {
			delete(db.pending, n)
		}
	}
	closeOutput := func() error {
		props, err := w.Finish()
		if err == nil {
			err = bw.Flush()
		}
		if err == nil {
			err = f.Sync()
		}
		if cerr := f.Close(); err == nil {
			err = cerr
		}
		w, bw, f = nil, nil, nil
		if err != nil {
			return err
		}
		outputs = append(outputs, manifest.FileMeta{
			Level:    c.output,
			FileNum:  num,
			Size:     props.FileSize,
			Smallest: props.Smallest,
			Largest:  props.Largest,
		})
		return nil
	}

	for m.First(); m.Valid(); m.Next() {
		ik, err := base.DecodeInternalKey(m.Key())
		if err != nil {
			finish(err)
			return
		}
		if haveUK && bytes.Equal(ik.UserKey, lastUK) {
			continue // shadowed by the newer version already handled
		}
		lastUK = append(lastUK[:0], ik.UserKey...)
		haveUK = true
		if ik.Kind == base.KindDelete && !c.keyMayExistBelow(ik.UserKey) {
			continue // tombstone GC
		}
		if w == nil {
			db.mu.Lock()
			num = db.vs.AllocFileNum()
			db.pending[num] = true
			outNums = append(outNums, num)
			db.mu.Unlock()
			var err error
			f, err = os.OpenFile(manifest.TableFileName(db.dir, num), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
			if err != nil {
				finish(err)
				return
			}
			bw = bufio.NewWriterSize(f, 64<<10)
			w = sstable.NewWriter(bw, sstable.WriterOptions{})
		}
		if err := w.Add(m.Key(), m.Value()); err != nil {
			finish(err)
			return
		}
		if w.EstimatedSize() >= uint64(db.opts.TargetFileSize) {
			if err := closeOutput(); err != nil {
				finish(err)
				return
			}
		}
	}
	if err := m.Error(); err != nil {
		finish(err)
		return
	}
	if w != nil {
		if err := closeOutput(); err != nil {
			finish(err)
			return
		}
	}
	if len(outputs) > 0 {
		if err := syncDir(db.dir); err != nil {
			finish(err)
			return
		}
	}
	if db.hookBeforeCompactApply != nil {
		if err := db.hookBeforeCompactApply(); err != nil {
			finish(err)
			return
		}
	}

	// Open handles for the outputs before taking mu; reads are lock-free.
	newHandles := make([]*tableHandle, 0, len(outputs))
	for _, meta := range outputs {
		h, err := db.openTable(meta)
		if err != nil {
			finish(err)
			return
		}
		newHandles = append(newHandles, h)
	}

	db.mu.Lock()
	edit := &manifest.VersionEdit{AddFiles: outputs}
	for side, metas := range c.inMeta {
		_ = side
		for _, m := range metas {
			edit.DeleteFiles = append(edit.DeleteFiles, manifest.DeletedFile{Level: m.Level, FileNum: m.FileNum})
		}
	}
	if err := db.vs.Apply(edit); err != nil {
		db.mu.Unlock()
		for _, h := range newHandles {
			h.unref()
		}
		finish(err)
		return
	}
	for _, h := range newHandles {
		db.tables[h.num] = h
	}
	if err := db.installVersionLocked(db.rs.mem, db.rs.imm); err != nil {
		db.mu.Unlock()
		finish(err)
		return
	}
	if db.hookAfterCompactApply != nil {
		if err := db.hookAfterCompactApply(); err != nil {
			db.mu.Unlock()
			finish(err)
			return
		}
	}
	clearPending()
	// Unlink dead inputs; pinned readers keep their fds alive until they
	// release. Pending numbers from a concurrent flush stay protected.
	if _, err := db.vs.DeleteObsolete(db.pending); err != nil {
		db.mu.Unlock()
		finish(err)
		return
	}
	db.mu.Unlock()
	finish(nil)
}
