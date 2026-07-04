package manifest

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/LovRanRan/basalt/internal/base"
	"github.com/LovRanRan/basalt/internal/wal"
)

var errBoom = errors.New("boom")

func fm(level int, num uint64, small, large string) FileMeta {
	return FileMeta{
		Level:    level,
		FileNum:  num,
		Size:     100 + num,
		Smallest: base.AppendInternalKey(nil, []byte(small), 10, base.KindPut),
		Largest:  base.AppendInternalKey(nil, []byte(large), 5, base.KindPut),
	}
}

// levelNums flattens a version into file numbers per level for comparison.
func levelNums(v *Version) [NumLevels][]uint64 {
	var out [NumLevels][]uint64
	for l, files := range v.Files {
		for _, f := range files {
			out[l] = append(out[l], f.FileNum)
		}
	}
	return out
}

func TestFreshOpenAndReopen(t *testing.T) {
	dir := t.TempDir()
	vs, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if len(vs.Current().Live()) != 0 {
		t.Fatal("fresh version must be empty")
	}
	if err := vs.Close(); err != nil {
		t.Fatal(err)
	}
	vs2, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = vs2.Close() }()
	if len(vs2.Current().Live()) != 0 {
		t.Fatal("reopened fresh version must be empty")
	}
	if _, err := os.Stat(filepath.Join(dir, currentName)); err != nil {
		t.Fatalf("CURRENT missing: %v", err)
	}
}

func TestEditRoundtrip(t *testing.T) {
	var e VersionEdit
	e.SetLogNumber(7)
	e.SetNextFileNum(42)
	e.SetLastSeq(1 << 50)
	e.AddFiles = []FileMeta{fm(0, 3, "a", "m"), fm(2, 9, "b\x00", "z\xff")}
	e.DeleteFiles = []DeletedFile{{Level: 1, FileNum: 4}}

	got, err := decodeEdit(e.encode(nil))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, e) {
		t.Fatalf("roundtrip mismatch:\ngot  %+v\nwant %+v", got, e)
	}
}

func TestDecodeEditRejectsCorruption(t *testing.T) {
	short := base.AppendInternalKey(nil, []byte("k"), 1, base.KindPut)
	okFile := FileMeta{Level: 1, FileNum: 2, Size: 3, Smallest: short, Largest: short}
	valid := (&VersionEdit{AddFiles: []FileMeta{okFile}}).encode(nil)

	cases := []struct {
		name    string
		payload []byte
	}{
		{"unknown tag", []byte{99}},
		{"truncated uvarint", []byte{tagLastSeq, 0x80}},
		{"level out of range", []byte{tagDeleteFile, 200, 1}},
		{"truncated add-file", valid[:len(valid)-3]},
		{"short internal key bound", (&VersionEdit{AddFiles: []FileMeta{{Level: 0, FileNum: 1, Smallest: []byte("xy"), Largest: short}}}).encode(nil)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := decodeEdit(tc.payload); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("err = %v, want ErrCorrupt", err)
			}
		})
	}
}

func TestApplyAndRecover(t *testing.T) {
	dir := t.TempDir()
	vs, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}

	e1 := &VersionEdit{AddFiles: []FileMeta{fm(0, vs.AllocFileNum(), "d", "f"), fm(0, vs.AllocFileNum(), "a", "c")}}
	if err := vs.Apply(e1); err != nil {
		t.Fatal(err)
	}
	e2 := &VersionEdit{AddFiles: []FileMeta{fm(1, vs.AllocFileNum(), "m", "p"), fm(1, vs.AllocFileNum(), "a", "c")}}
	e2.SetLastSeq(4242)
	e2.SetLogNumber(9)
	if err := vs.Apply(e2); err != nil {
		t.Fatal(err)
	}
	first := vs.Current().Files[0][0].FileNum
	e3 := &VersionEdit{DeleteFiles: []DeletedFile{{Level: 0, FileNum: first}}}
	if err := vs.Apply(e3); err != nil {
		t.Fatal(err)
	}

	want := levelNums(vs.Current())
	if err := vs.Close(); err != nil {
		t.Fatal(err)
	}

	vs2, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = vs2.Close() }()
	if got := levelNums(vs2.Current()); !reflect.DeepEqual(got, want) {
		t.Fatalf("recovered version = %v, want %v", got, want)
	}
	if vs2.LastSeq() != 4242 || vs2.LogNumber() != 9 {
		t.Fatalf("recovered counters: lastSeq=%d logNumber=%d", vs2.LastSeq(), vs2.LogNumber())
	}
	// L0 newest first; L1 by smallest key.
	l0 := vs2.Current().Files[0]
	if len(l0) != 1 {
		t.Fatalf("L0 = %d files, want 1", len(l0))
	}
	l1 := vs2.Current().Files[1]
	if len(l1) != 2 || base.Compare(l1[0].Smallest, l1[1].Smallest) >= 0 {
		t.Fatalf("L1 not sorted by smallest key")
	}
}

func TestApplyInvalidEditFailsSticky(t *testing.T) {
	dir := t.TempDir()
	vs, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = vs.Close() }()
	bad := &VersionEdit{DeleteFiles: []DeletedFile{{Level: 0, FileNum: 999}}}
	if err := vs.Apply(bad); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("deleting a non-live file = %v, want ErrCorrupt", err)
	}
	ok := &VersionEdit{AddFiles: []FileMeta{fm(0, 50, "a", "b")}}
	if err := vs.Apply(ok); err == nil {
		t.Fatal("version set must stay failed after an invalid edit")
	}
}

func TestCrashDuringRotationRecoversConsistently(t *testing.T) {
	steps := []string{"write-manifest", "sync-manifest", "sync-dir", "sync-tmp", "rename-current"}
	for _, step := range steps {
		t.Run(step, func(t *testing.T) {
			dir := t.TempDir()
			vs, err := Open(dir, Options{RotateSize: 1}) // every Apply rotates
			if err != nil {
				t.Fatal(err)
			}
			if err := vs.Apply(&VersionEdit{AddFiles: []FileMeta{fm(0, vs.AllocFileNum(), "a", "b")}}); err != nil {
				t.Fatal(err)
			}
			if err := vs.Apply(&VersionEdit{AddFiles: []FileMeta{fm(1, vs.AllocFileNum(), "c", "d")}}); err != nil {
				t.Fatal(err)
			}

			// The next Apply logs its edit durably to the old manifest,
			// then crashes at `step` inside the triggered rotation — so
			// recovery must include all three edits.
			vs.failpoint = func(s string) error {
				if s == step {
					return errBoom
				}
				return nil
			}
			third := &VersionEdit{AddFiles: []FileMeta{fm(2, vs.AllocFileNum(), "e", "f")}}
			third.SetLastSeq(777)
			// The edit itself is durable before rotation begins, so the
			// crashed rotation must not be reported against it.
			if err := vs.Apply(third); err != nil {
				t.Fatalf("Apply whose rotation crashed = %v, want nil (edit committed)", err)
			}
			want := levelNums(vs.Current())
			if err := vs.Apply(&VersionEdit{}); !errors.Is(err, errBoom) {
				t.Fatalf("next Apply = %v, want sticky errBoom", err)
			}

			vs2, err := Open(dir, Options{})
			if err != nil {
				t.Fatalf("reopen after crash at %s: %v", step, err)
			}
			defer func() { _ = vs2.Close() }()
			if got := levelNums(vs2.Current()); !reflect.DeepEqual(got, want) {
				t.Fatalf("recovered version = %v, want %v", got, want)
			}
			if vs2.LastSeq() != 777 {
				t.Fatalf("recovered lastSeq = %d, want 777", vs2.LastSeq())
			}
			if _, err := vs2.DeleteObsolete(); err != nil {
				t.Fatal(err)
			}
			if err := vs2.Apply(&VersionEdit{AddFiles: []FileMeta{fm(3, vs2.AllocFileNum(), "g", "h")}}); err != nil {
				t.Fatalf("apply after recovery: %v", err)
			}
		})
	}
}

func TestCollectorNeverDeletesLive(t *testing.T) {
	dir := t.TempDir()
	vs, err := Open(dir, Options{RotateSize: 256})
	if err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewSource(0xba5a17))
	live := map[uint64]FileMeta{}

	for i := 0; i < 200; i++ {
		var e VersionEdit
		for j := 0; j < 1+rng.Intn(3); j++ {
			num := vs.AllocFileNum()
			f := fm(rng.Intn(NumLevels), num, fmt.Sprintf("k%06d", num), fmt.Sprintf("k%06d~", num))
			if err := os.WriteFile(TableFileName(dir, num), []byte("table"), 0o644); err != nil {
				t.Fatal(err)
			}
			e.AddFiles = append(e.AddFiles, f)
		}
		if len(live) > 0 && rng.Intn(2) == 0 {
			for num, f := range live {
				e.DeleteFiles = append(e.DeleteFiles, DeletedFile{Level: f.Level, FileNum: num})
				break
			}
		}
		if err := vs.Apply(&e); err != nil {
			t.Fatal(err)
		}
		for _, f := range e.AddFiles {
			live[f.FileNum] = f
		}
		for _, d := range e.DeleteFiles {
			delete(live, d.FileNum)
		}

		if rng.Intn(4) == 0 {
			if _, err := vs.DeleteObsolete(); err != nil {
				t.Fatal(err)
			}
			for num := range live {
				if _, err := os.Stat(TableFileName(dir, num)); err != nil {
					t.Fatalf("iteration %d: live table %06d deleted: %v", i, num, err)
				}
			}
		}
	}
	want := levelNums(vs.Current())
	if err := vs.Close(); err != nil {
		t.Fatal(err)
	}
	vs2, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = vs2.Close() }()
	if got := levelNums(vs2.Current()); !reflect.DeepEqual(got, want) {
		t.Fatal("recovered version diverges from tracked state")
	}
	if len(vs2.Current().Live()) != len(live) {
		t.Fatalf("live files = %d, want %d", len(vs2.Current().Live()), len(live))
	}
}

func TestManifestTailShapes(t *testing.T) {
	build := t.TempDir()
	vs, err := Open(build, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := vs.Apply(&VersionEdit{AddFiles: []FileMeta{fm(0, vs.AllocFileNum(), "a", "b")}}); err != nil {
		t.Fatal(err)
	}
	afterFirst := levelNums(vs.Current())
	if err := vs.Apply(&VersionEdit{AddFiles: []FileMeta{fm(1, vs.AllocFileNum(), "c", "d")}}); err != nil {
		t.Fatal(err)
	}
	name := manifestName(vs.ManifestNum())
	if err := vs.Close(); err != nil {
		t.Fatal(err)
	}
	orig, err := os.ReadFile(filepath.Join(build, name))
	if err != nil {
		t.Fatal(err)
	}

	// Locate the final record's start with the real parser.
	lastStart, off := 0, 0
	rest := orig
	for len(rest) > 0 {
		_, next, ok := wal.NextRecord(rest)
		if !ok {
			t.Fatal("pristine manifest failed to parse")
		}
		lastStart = off
		off += len(rest) - len(next)
		rest = next
	}

	openState := func(contents []byte) ([NumLevels][]uint64, error) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, name), contents, 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, currentName), []byte(name+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		v, err := Open(dir, Options{})
		if err != nil {
			return [NumLevels][]uint64{}, err
		}
		defer func() { _ = v.Close() }()
		return levelNums(v.Current()), nil
	}

	// Torn shapes of the final record: recover everything before it.
	for _, cut := range []int{lastStart + 1, lastStart + wal.RecordHeaderLen + 1, len(orig) - 1} {
		got, err := openState(orig[:cut])
		if err != nil {
			t.Fatalf("cut@%d: %v", cut, err)
		}
		if !reflect.DeepEqual(got, afterFirst) {
			t.Fatalf("cut@%d recovered %v, want %v", cut, got, afterFirst)
		}
	}
	// Complete frame, bad crc, exactly at EOF: lenient (size can persist
	// before data in a crash).
	eofFlip := append([]byte(nil), orig...)
	eofFlip[len(eofFlip)-1] ^= 0x01
	if got, err := openState(eofFlip); err != nil || !reflect.DeepEqual(got, afterFirst) {
		t.Fatalf("eof crc flip: got %v err %v", got, err)
	}
	// Damage before the end with valid records after it is provably not a
	// torn tail and must refuse to open.
	midFlip := append([]byte(nil), orig...)
	midFlip[wal.RecordHeaderLen+1] ^= 0x01
	if _, err := openState(midFlip); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("mid-file damage: err = %v, want ErrCorrupt", err)
	}
}

func TestMissingCurrentWithTablesRefusesOpen(t *testing.T) {
	dir := t.TempDir()
	vs, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	num := vs.AllocFileNum()
	if err := os.WriteFile(TableFileName(dir, num), []byte("t"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := vs.Apply(&VersionEdit{AddFiles: []FileMeta{fm(0, num, "a", "b")}}); err != nil {
		t.Fatal(err)
	}
	if err := vs.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, currentName)); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir, Options{}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("missing CURRENT with tables = %v, want ErrCorrupt", err)
	}
}

func TestOpenBumpsPastOrphansAndCollects(t *testing.T) {
	dir := t.TempDir()
	vs, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	num := vs.AllocFileNum()
	if err := os.WriteFile(TableFileName(dir, num), []byte("live"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := vs.Apply(&VersionEdit{AddFiles: []FileMeta{fm(0, num, "a", "b")}}); err != nil {
		t.Fatal(err)
	}
	if err := vs.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(TableFileName(dir, 99), []byte("orphan"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, manifestName(98)), []byte("orphan"), 0o644); err != nil {
		t.Fatal(err)
	}

	vs2, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = vs2.Close() }()
	if vs2.ManifestNum() < 100 {
		t.Fatalf("rotation must number past the orphans, got %d", vs2.ManifestNum())
	}
	if n := vs2.AllocFileNum(); n < 100 {
		t.Fatalf("AllocFileNum = %d, must never reuse an orphan number", n)
	}
	if _, err := os.Stat(TableFileName(dir, 99)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("orphan table must be collected at open")
	}
	if _, err := os.Stat(filepath.Join(dir, manifestName(98))); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("orphan manifest must be collected at open")
	}
	if _, err := os.Stat(TableFileName(dir, num)); err != nil {
		t.Fatalf("live table must survive: %v", err)
	}
}

func TestLevelMoveInOneEdit(t *testing.T) {
	dir := t.TempDir()
	vs, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = vs.Close() }()
	num := vs.AllocFileNum()
	if err := vs.Apply(&VersionEdit{AddFiles: []FileMeta{fm(0, num, "a", "b")}}); err != nil {
		t.Fatal(err)
	}
	move := &VersionEdit{
		DeleteFiles: []DeletedFile{{Level: 0, FileNum: num}},
		AddFiles:    []FileMeta{fm(1, num, "a", "b")},
	}
	if err := vs.Apply(move); err != nil {
		t.Fatalf("delete+add of the same file number in one edit: %v", err)
	}
	v := vs.Current()
	if len(v.Files[0]) != 0 || len(v.Files[1]) != 1 || v.Files[1][0].FileNum != num {
		t.Fatalf("level move failed: %v", levelNums(v))
	}
}

func TestApplyRejectsCounterRegression(t *testing.T) {
	dir := t.TempDir()
	vs, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = vs.Close() }()
	adv := &VersionEdit{}
	adv.SetLastSeq(100)
	adv.SetLogNumber(5)
	if err := vs.Apply(adv); err != nil {
		t.Fatal(err)
	}
	back := &VersionEdit{}
	back.SetLastSeq(50)
	if err := vs.Apply(back); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("lastSeq regression = %v, want ErrCorrupt", err)
	}

	dir2 := t.TempDir()
	vs2, err := Open(dir2, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = vs2.Close() }()
	adv2 := &VersionEdit{}
	adv2.SetLogNumber(9)
	if err := vs2.Apply(adv2); err != nil {
		t.Fatal(err)
	}
	back2 := &VersionEdit{}
	back2.SetLogNumber(3)
	if err := vs2.Apply(back2); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("logNumber regression = %v, want ErrCorrupt", err)
	}
}

func TestApplyRejectsOverlappingLevelFiles(t *testing.T) {
	dir := t.TempDir()
	vs, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = vs.Close() }()
	e := &VersionEdit{AddFiles: []FileMeta{
		fm(1, vs.AllocFileNum(), "a", "m"),
		fm(1, vs.AllocFileNum(), "g", "z"),
	}}
	if err := vs.Apply(e); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("overlapping L1 files = %v, want ErrCorrupt", err)
	}
}

func TestDoubleCrashRecovery(t *testing.T) {
	steps := []string{"write-manifest", "sync-manifest", "sync-dir", "sync-tmp", "rename-current"}
	dir := t.TempDir()
	vs, err := Open(dir, Options{RotateSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := vs.Apply(&VersionEdit{AddFiles: []FileMeta{fm(0, vs.AllocFileNum(), "a", "b")}}); err != nil {
		t.Fatal(err)
	}
	vs.failpoint = func(s string) error {
		if s == "sync-tmp" {
			return errBoom
		}
		return nil
	}
	if err := vs.Apply(&VersionEdit{AddFiles: []FileMeta{fm(1, vs.AllocFileNum(), "c", "d")}}); err != nil {
		t.Fatal(err)
	}
	want := levelNums(vs.Current())

	// Every recovery attempt crashes inside its own recovery rotation.
	for _, step := range steps {
		step := step
		if _, err := Open(dir, Options{failpoint: func(s string) error {
			if s == step {
				return errBoom
			}
			return nil
		}}); err == nil {
			t.Fatalf("recovery open crashing at %s must fail", step)
		}
	}

	vs3, err := Open(dir, Options{})
	if err != nil {
		t.Fatalf("final recovery: %v", err)
	}
	defer func() { _ = vs3.Close() }()
	if got := levelNums(vs3.Current()); !reflect.DeepEqual(got, want) {
		t.Fatalf("after double crashes recovered %v, want %v", got, want)
	}
	if err := vs3.Apply(&VersionEdit{AddFiles: []FileMeta{fm(2, vs3.AllocFileNum(), "e", "f")}}); err != nil {
		t.Fatal(err)
	}
}
