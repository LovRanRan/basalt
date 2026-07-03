package wal

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// collect replays dir and returns copies of every committed payload.
func collect(t *testing.T, dir string) [][]byte {
	t.Helper()
	var got [][]byte
	err := Replay(dir, func(p []byte) error {
		got = append(got, append([]byte(nil), p...))
		return nil
	})
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	return got
}

func mustAppend(t *testing.T, w *Writer, p []byte) {
	t.Helper()
	if _, err := w.Append(p); err != nil {
		t.Fatalf("append: %v", err)
	}
}

func TestRecordRoundtrip(t *testing.T) {
	payloads := [][]byte{nil, {}, []byte("x"), bytes.Repeat([]byte{0xa5}, 1000)}
	var buf []byte
	for _, p := range payloads {
		buf = AppendRecord(buf, p)
	}
	rest := buf
	for i, want := range payloads {
		var got []byte
		var ok bool
		got, rest, ok = NextRecord(rest)
		if !ok {
			t.Fatalf("record %d: unexpected parse failure", i)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("record %d: got %d bytes, want %d", i, len(got), len(want))
		}
	}
	if len(rest) != 0 {
		t.Fatalf("%d trailing bytes", len(rest))
	}
}

func TestTornTailRecoveryExhaustive(t *testing.T) {
	build := t.TempDir()
	w, err := OpenWriter(Options{Dir: build, Sync: SyncManual})
	if err != nil {
		t.Fatal(err)
	}
	var payloads [][]byte
	for i := 0; i < 5; i++ {
		p := []byte(fmt.Sprintf("record-%d-%s", i, strings.Repeat("x", 20+i*7)))
		payloads = append(payloads, p)
		mustAppend(t, w, p)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	orig, err := os.ReadFile(filepath.Join(build, segmentName(1)))
	if err != nil {
		t.Fatal(err)
	}

	// Record boundaries, via the real parser.
	var starts []int
	off := 0
	rest := orig
	for len(rest) > 0 {
		starts = append(starts, off)
		_, next, ok := NextRecord(rest)
		if !ok {
			t.Fatal("pristine segment failed to parse")
		}
		off += len(rest) - len(next)
		rest = next
	}
	if len(starts) != 5 {
		t.Fatalf("expected 5 records, got %d", len(starts))
	}
	starts = append(starts, len(orig))
	recordOf := func(off int) int {
		i := 0
		for off >= starts[i+1] {
			i++
		}
		return i
	}

	scratch := t.TempDir()
	seg := filepath.Join(scratch, segmentName(1))
	check := func(t *testing.T, mutated []byte, wantN int, what string) {
		t.Helper()
		if err := os.WriteFile(seg, mutated, 0o644); err != nil {
			t.Fatal(err)
		}
		got := collect(t, scratch)
		if len(got) != wantN {
			t.Fatalf("%s: recovered %d records, want %d", what, len(got), wantN)
		}
		for i := 0; i < wantN; i++ {
			if !bytes.Equal(got[i], payloads[i]) {
				t.Fatalf("%s: record %d = %q, want %q", what, i, got[i], payloads[i])
			}
		}
	}

	check(t, orig, 5, "pristine")
	// Damage anywhere in the newest segment — not just its tail — drops
	// everything from the damaged record onward: mid-segment corruption
	// and a torn tail are indistinguishable there. Replay must return the
	// exact clean prefix, no error, never a mangled record.
	for cut := 0; cut < len(orig); cut++ {
		check(t, orig[:cut], recordOf(cut), fmt.Sprintf("truncate@%d", cut))
	}
	for off := 0; off < len(orig); off++ {
		mut := append([]byte(nil), orig...)
		mut[off] ^= 0x01
		check(t, mut, recordOf(off), fmt.Sprintf("bitflip@%d", off))
	}
}

func TestMidLogCorruptionIsLoud(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWriter(Options{Dir: dir, Sync: SyncManual})
	if err != nil {
		t.Fatal(err)
	}
	mustAppend(t, w, []byte("old segment record"))
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	w2, err := OpenWriter(Options{Dir: dir, Sync: SyncManual})
	if err != nil {
		t.Fatal(err)
	}
	mustAppend(t, w2, []byte("new segment record"))
	if err := w2.Close(); err != nil {
		t.Fatal(err)
	}

	seg1 := filepath.Join(dir, segmentName(1))
	buf, err := os.ReadFile(seg1)
	if err != nil {
		t.Fatal(err)
	}
	buf[RecordHeaderLen] ^= 0x01
	if err := os.WriteFile(seg1, buf, 0o644); err != nil {
		t.Fatal(err)
	}
	err = Replay(dir, func([]byte) error { return nil })
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("replay err = %v, want ErrCorrupt", err)
	}
}

func TestRotationAndOrderedReplay(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWriter(Options{Dir: dir, SegmentSize: 64, Sync: SyncManual})
	if err != nil {
		t.Fatal(err)
	}
	const n = 50
	for i := 0; i < n; i++ {
		mustAppend(t, w, []byte(fmt.Sprintf("record-%02d", i)))
	}
	if w.SegmentID() < 3 {
		t.Fatalf("expected multiple rotations, current segment id = %d", w.SegmentID())
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	got := collect(t, dir)
	if len(got) != n {
		t.Fatalf("replayed %d records, want %d", len(got), n)
	}
	for i, p := range got {
		if want := fmt.Sprintf("record-%02d", i); string(p) != want {
			t.Fatalf("record %d = %q, want %q", i, p, want)
		}
	}
}

func TestReopenStartsFreshSegment(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWriter(Options{Dir: dir, Sync: SyncManual})
	if err != nil {
		t.Fatal(err)
	}
	mustAppend(t, w, []byte("first"))
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	w2, err := OpenWriter(Options{Dir: dir, Sync: SyncEveryRecord})
	if err != nil {
		t.Fatal(err)
	}
	if w2.SegmentID() != 2 {
		t.Fatalf("reopened writer segment id = %d, want 2", w2.SegmentID())
	}
	mustAppend(t, w2, []byte("second"))
	if err := w2.Close(); err != nil {
		t.Fatal(err)
	}

	got := collect(t, dir)
	if len(got) != 2 || string(got[0]) != "first" || string(got[1]) != "second" {
		t.Fatalf("replay = %q", got)
	}
}

func TestDeleteSegmentsBefore(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWriter(Options{Dir: dir, SegmentSize: 32, Sync: SyncManual})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		mustAppend(t, w, []byte(fmt.Sprintf("record-%02d", i)))
	}
	cur := w.SegmentID()
	if cur < 3 {
		t.Fatalf("expected rotations, segment id = %d", cur)
	}
	if err := w.DeleteSegmentsBefore(cur); err != nil {
		t.Fatal(err)
	}
	ids, err := listSegments(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 || ids[0] != cur {
		t.Fatalf("segments after delete = %v, want only %d", ids, cur)
	}
	mustAppend(t, w, []byte("after-delete"))
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	got := collect(t, dir)
	if len(got) == 0 || string(got[len(got)-1]) != "after-delete" {
		t.Fatalf("replay after delete = %q", got)
	}
}

func TestReopenRepairsTornTailAcrossRestarts(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWriter(Options{Dir: dir, Sync: SyncManual})
	if err != nil {
		t.Fatal(err)
	}
	mustAppend(t, w, []byte("committed-1"))
	mustAppend(t, w, []byte("committed-2"))
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Simulate a crash mid-append: a half-written record at the tail.
	seg := filepath.Join(dir, segmentName(1))
	f, err := os.OpenFile(seg, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{0xde, 0xad, 0xbe}); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	torn, err := os.Stat(seg)
	if err != nil {
		t.Fatal(err)
	}

	// First restart repairs the tail before opening segment 2.
	w2, err := OpenWriter(Options{Dir: dir, Sync: SyncManual})
	if err != nil {
		t.Fatal(err)
	}
	repaired, err := os.Stat(seg)
	if err != nil {
		t.Fatal(err)
	}
	if repaired.Size() != torn.Size()-3 {
		t.Fatalf("segment 1 size = %d, want torn tail truncated to %d", repaired.Size(), torn.Size()-3)
	}
	mustAppend(t, w2, []byte("after-crash"))
	if err := w2.Close(); err != nil {
		t.Fatal(err)
	}

	// Second restart: segment 1 is no longer the newest, and must replay
	// clean rather than surfacing its old torn tail as ErrCorrupt.
	got := collect(t, dir)
	want := []string{"committed-1", "committed-2", "after-crash"}
	if len(got) != len(want) {
		t.Fatalf("replayed %d records %q, want %d", len(got), got, len(want))
	}
	for i := range want {
		if string(got[i]) != want[i] {
			t.Fatalf("record %d = %q, want %q", i, got[i], want[i])
		}
	}
	w3, err := OpenWriter(Options{Dir: dir, Sync: SyncManual})
	if err != nil {
		t.Fatal(err)
	}
	if w3.SegmentID() != 3 {
		t.Fatalf("third incarnation segment id = %d, want 3", w3.SegmentID())
	}
	if err := w3.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestOpenWriterTruncatesMidSegmentDamage(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWriter(Options{Dir: dir, Sync: SyncManual})
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{"r0", "r1", "r2"} {
		mustAppend(t, w, []byte(p))
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	seg := filepath.Join(dir, segmentName(1))
	orig, err := os.ReadFile(seg)
	if err != nil {
		t.Fatal(err)
	}
	_, rest, ok := NextRecord(orig)
	if !ok {
		t.Fatal("first record must parse")
	}
	start1 := len(orig) - len(rest)
	orig[start1] ^= 0x01
	if err := os.WriteFile(seg, orig, 0o644); err != nil {
		t.Fatal(err)
	}

	w2, err := OpenWriter(Options{Dir: dir, Sync: SyncManual})
	if err != nil {
		t.Fatal(err)
	}
	st, err := os.Stat(seg)
	if err != nil {
		t.Fatal(err)
	}
	if st.Size() != int64(start1) {
		t.Fatalf("segment 1 size = %d, want truncated to %d", st.Size(), start1)
	}
	mustAppend(t, w2, []byte("after"))
	if err := w2.Close(); err != nil {
		t.Fatal(err)
	}

	got := collect(t, dir)
	if len(got) != 2 || string(got[0]) != "r0" || string(got[1]) != "after" {
		t.Fatalf("replay = %q, want [r0 after]", got)
	}
}

func TestWriterPoisonedAfterFailure(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWriter(Options{Dir: dir, Sync: SyncManual})
	if err != nil {
		t.Fatal(err)
	}
	mustAppend(t, w, []byte("ok"))
	w.f.Close() // the device fails underneath the writer
	if _, err := w.Append([]byte("fails")); err == nil {
		t.Fatal("append on a failed file must error")
	}
	if _, err := w.Append([]byte("still fails")); err == nil || !strings.Contains(err.Error(), "writer failed") {
		t.Fatalf("second append = %v, want sticky writer-failed error", err)
	}
	if err := w.Sync(); err == nil || !strings.Contains(err.Error(), "writer failed") {
		t.Fatalf("sync after failure = %v, want sticky writer-failed error", err)
	}
}

func TestRecordTooLargeIsCleanRejection(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWriter(Options{Dir: dir, MaxRecordSize: 16, Sync: SyncManual})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Append(bytes.Repeat([]byte{'x'}, 17)); !errors.Is(err, ErrRecordTooLarge) {
		t.Fatalf("oversized append = %v, want ErrRecordTooLarge", err)
	}
	mustAppend(t, w, []byte("small"))
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	got := collect(t, dir)
	if len(got) != 1 || string(got[0]) != "small" {
		t.Fatalf("replay = %q, want [small]", got)
	}
}

func TestReplayDetectsMissingSegment(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWriter(Options{Dir: dir, SegmentSize: 32, Sync: SyncManual})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		mustAppend(t, w, []byte(fmt.Sprintf("record-%02d", i)))
	}
	if w.SegmentID() < 3 {
		t.Fatalf("expected rotations, segment id = %d", w.SegmentID())
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, segmentName(2))); err != nil {
		t.Fatal(err)
	}
	err = Replay(dir, func([]byte) error { return nil })
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("replay with missing segment = %v, want ErrCorrupt", err)
	}
}

func TestReplayMissingAndEmptyDir(t *testing.T) {
	if err := Replay(filepath.Join(t.TempDir(), "nope"), func([]byte) error {
		t.Fatal("callback on missing dir")
		return nil
	}); err != nil {
		t.Fatalf("missing dir: %v", err)
	}
	if got := collect(t, t.TempDir()); len(got) != 0 {
		t.Fatalf("empty dir replayed %d records", len(got))
	}
}
