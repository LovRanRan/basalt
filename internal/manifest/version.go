package manifest

import (
	"slices"
	"sort"

	"github.com/LovRanRan/basalt/internal/base"
)

// Version is an immutable snapshot of the live tables per level. L0 files
// may overlap and are ordered newest first (descending file number); deeper
// levels are ordered by smallest key.
type Version struct {
	Files [NumLevels][]FileMeta
}

// Live returns the set of live table file numbers.
func (v *Version) Live() map[uint64]bool {
	live := map[uint64]bool{}
	for _, level := range v.Files {
		for _, f := range level {
			live[f.FileNum] = true
		}
	}
	return live
}

// applyEdit returns a new Version with the edit applied. Deleting an absent
// file or re-adding a live file number is a caller logic error surfaced as
// an error so the VersionSet can refuse to log a self-inconsistent edit.
func applyEdit(v *Version, e *VersionEdit) (*Version, error) {
	nv := &Version{}
	for l := range v.Files {
		nv.Files[l] = slices.Clone(v.Files[l])
	}
	for _, d := range e.DeleteFiles {
		if d.Level < 0 || d.Level >= NumLevels {
			return nil, corruptf("delete-file level %d out of range", d.Level)
		}
		i := slices.IndexFunc(nv.Files[d.Level], func(f FileMeta) bool { return f.FileNum == d.FileNum })
		if i < 0 {
			return nil, corruptf("delete of file %06d not live at level %d", d.FileNum, d.Level)
		}
		nv.Files[d.Level] = slices.Delete(nv.Files[d.Level], i, i+1)
	}
	live := nv.Live()
	for _, f := range e.AddFiles {
		if f.Level < 0 || f.Level >= NumLevels {
			return nil, corruptf("add-file level %d out of range", f.Level)
		}
		if live[f.FileNum] {
			return nil, corruptf("add of file %06d already live", f.FileNum)
		}
		live[f.FileNum] = true
		nv.Files[f.Level] = append(nv.Files[f.Level], f)
	}
	for _, f := range e.AddFiles {
		if base.Compare(f.Smallest, f.Largest) > 0 {
			return nil, corruptf("file %06d has smallest > largest", f.FileNum)
		}
	}
	for l := range nv.Files {
		if l == 0 {
			sort.Slice(nv.Files[0], func(i, j int) bool { return nv.Files[0][i].FileNum > nv.Files[0][j].FileNum })
			continue
		}
		sort.Slice(nv.Files[l], func(i, j int) bool {
			return base.Compare(nv.Files[l][i].Smallest, nv.Files[l][j].Smallest) < 0
		})
		// Levels below L0 must hold disjoint key ranges — that is the
		// invariant the whole read path binary-searches on, so a
		// compaction edit that would break it is refused at the commit
		// boundary.
		for i := 0; i+1 < len(nv.Files[l]); i++ {
			if base.Compare(nv.Files[l][i].Largest, nv.Files[l][i+1].Smallest) >= 0 {
				return nil, corruptf("level %d files %06d and %06d overlap", l, nv.Files[l][i].FileNum, nv.Files[l][i+1].FileNum)
			}
		}
	}
	return nv, nil
}

// snapshot encodes the whole version as one edit — the first record of a
// fresh manifest.
func snapshot(v *Version) *VersionEdit {
	var e VersionEdit
	for _, level := range v.Files {
		e.AddFiles = append(e.AddFiles, level...)
	}
	return &e
}
