package base_test

import (
	"bytes"
	"errors"
	"math/rand"
	"sort"
	"testing"

	"github.com/LovRanRan/basalt/internal/base"
)

func TestInternalKeyRoundtrip(t *testing.T) {
	cases := []struct {
		name    string
		userKey []byte
		seq     uint64
		kind    base.Kind
	}{
		{"empty user key", nil, 0, base.KindDelete},
		{"ascii key", []byte("user:1001"), 42, base.KindPut},
		{"binary extremes", []byte{0x00, 0xff, 0x00}, 1, base.KindPut},
		{"max seq", []byte("k"), base.MaxSeq, base.KindDelete},
		{"seq below max", []byte("k"), base.MaxSeq - 1, base.KindPut},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			enc := base.AppendInternalKey(nil, tc.userKey, tc.seq, tc.kind)
			if got, want := len(enc), len(tc.userKey)+base.TrailerLen; got != want {
				t.Fatalf("encoded length = %d, want %d", got, want)
			}
			ik, err := base.DecodeInternalKey(enc)
			if err != nil {
				t.Fatalf("DecodeInternalKey: %v", err)
			}
			if !bytes.Equal(ik.UserKey, tc.userKey) {
				t.Errorf("UserKey = %q, want %q", ik.UserKey, tc.userKey)
			}
			if ik.Seq != tc.seq {
				t.Errorf("Seq = %d, want %d", ik.Seq, tc.seq)
			}
			if ik.Kind != tc.kind {
				t.Errorf("Kind = %d, want %d", ik.Kind, tc.kind)
			}
			if got := base.UserKey(enc); !bytes.Equal(got, tc.userKey) {
				t.Errorf("base.UserKey = %q, want %q", got, tc.userKey)
			}
		})
	}
}

func TestDecodeInternalKeyErrors(t *testing.T) {
	if _, err := base.DecodeInternalKey([]byte("short")); !errors.Is(err, base.ErrKeyTooShort) {
		t.Errorf("short input: err = %v, want ErrKeyTooShort", err)
	}
	enc := base.AppendInternalKey(nil, []byte("k"), 7, base.KindPut)
	enc[len(enc)-base.TrailerLen] = 0x7f
	if _, err := base.DecodeInternalKey(enc); !errors.Is(err, base.ErrInvalidKind) {
		t.Errorf("bad kind byte: err = %v, want ErrInvalidKind", err)
	}
}

func TestAppendInternalKeyRejectsOversizedSeq(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic for seq > MaxSeq")
		}
	}()
	base.AppendInternalKey(nil, []byte("k"), base.MaxSeq+1, base.KindPut)
}

func TestUserKeyAppendDoesNotClobberTrailer(t *testing.T) {
	enc := base.AppendInternalKey(nil, []byte("key"), 9, base.KindPut)
	_ = append(base.UserKey(enc), 0xee)
	ik, err := base.DecodeInternalKey(enc)
	if err != nil || ik.Seq != 9 || ik.Kind != base.KindPut {
		t.Fatalf("trailer corrupted by append through UserKey: %+v, err = %v", ik, err)
	}
}

func TestCompareProperties(t *testing.T) {
	rng := rand.New(rand.NewSource(0xba5a17))
	const n = 10000
	keys := make([][]byte, n)
	for i := range keys {
		uk := make([]byte, rng.Intn(9))
		for j := range uk {
			uk[j] = byte('a' + rng.Intn(3))
		}
		seq := rng.Uint64() & base.MaxSeq
		switch rng.Intn(10) {
		case 0:
			seq = 0
		case 1:
			seq = base.MaxSeq
		}
		keys[i] = base.AppendInternalKey(nil, uk, seq, base.Kind(rng.Intn(2)))
	}

	for i := 0; i < 1000; i++ {
		a, b := keys[rng.Intn(n)], keys[rng.Intn(n)]
		if base.Compare(a, b) != -base.Compare(b, a) {
			t.Fatalf("Compare not antisymmetric for %x / %x", a, b)
		}
	}

	sort.Slice(keys, func(i, j int) bool { return base.Compare(keys[i], keys[j]) < 0 })
	for i := 1; i < n; i++ {
		prev, cur := mustDecode(t, keys[i-1]), mustDecode(t, keys[i])
		switch c := bytes.Compare(prev.UserKey, cur.UserKey); {
		case c > 0:
			t.Fatalf("user keys not ascending at %d: %q > %q", i, prev.UserKey, cur.UserKey)
		case c == 0:
			if prev.Seq < cur.Seq {
				t.Fatalf("seq not descending for %q at %d: %d before %d", cur.UserKey, i, prev.Seq, cur.Seq)
			}
			if prev.Seq == cur.Seq && prev.Kind < cur.Kind {
				t.Fatalf("kind not descending for %q seq %d at %d", cur.UserKey, prev.Seq, i)
			}
		}
	}
}

func TestAppendInternalKeyRejectsInvalidKind(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic for invalid kind")
		}
	}()
	base.AppendInternalKey(nil, []byte("k"), 1, base.Kind(0x7f))
}

func TestCompareOrderingCases(t *testing.T) {
	k := func(uk string, seq uint64, kind base.Kind) []byte {
		return base.AppendInternalKey(nil, []byte(uk), seq, kind)
	}
	cases := []struct {
		name string
		a, b []byte
		want int
	}{
		{"user key ascending", k("a", 1, base.KindPut), k("b", 100, base.KindPut), -1},
		{"prefix sorts first", k("a", 1, base.KindPut), k("ab", 100, base.KindPut), -1},
		{"empty user key sorts first", k("", 1, base.KindPut), k("a", 100, base.KindPut), -1},
		{"same user key: higher seq first", k("a", 9, base.KindPut), k("a", 8, base.KindPut), -1},
		{"same user key and seq: put before delete", k("a", 9, base.KindPut), k("a", 9, base.KindDelete), -1},
		{"identical keys equal", k("a", 9, base.KindDelete), k("a", 9, base.KindDelete), 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := base.Compare(tc.a, tc.b); got != tc.want {
				t.Errorf("Compare = %d, want %d", got, tc.want)
			}
			if got := base.Compare(tc.b, tc.a); got != -tc.want {
				t.Errorf("reversed Compare = %d, want %d", got, -tc.want)
			}
		})
	}
}

func TestSeekKeyPositionsAtNewestVisible(t *testing.T) {
	put := base.AppendInternalKey(nil, []byte("k"), 5, base.KindPut)
	del := base.AppendInternalKey(nil, []byte("k"), 5, base.KindDelete)
	seek := base.AppendSeekKey(nil, []byte("k"), 5)
	if base.Compare(seek, put) > 0 {
		t.Errorf("seek key sorts after the Put at its own seq — SeekGE would skip it")
	}
	if base.Compare(seek, del) > 0 {
		t.Errorf("seek key sorts after the Delete at its own seq — SeekGE would skip it")
	}
	newer := base.AppendInternalKey(nil, []byte("k"), 6, base.KindPut)
	if base.Compare(seek, newer) <= 0 {
		t.Errorf("seek key must sort after versions newer than its snapshot seq")
	}
}

func mustDecode(t *testing.T, enc []byte) base.InternalKey {
	t.Helper()
	ik, err := base.DecodeInternalKey(enc)
	if err != nil {
		t.Fatalf("DecodeInternalKey(%x): %v", enc, err)
	}
	return ik
}
