package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/LovRanRan/basalt/internal/base"
)

func TestBatchRoundtrip(t *testing.T) {
	cases := []struct {
		name  string
		build func() Batch
	}{
		{"empty batch", func() Batch { return Batch{BaseSeq: 7} }},
		{"single put", func() Batch {
			b := Batch{BaseSeq: 1}
			b.Put([]byte("k"), []byte("v"))
			return b
		}},
		{"put empty value", func() Batch {
			b := Batch{BaseSeq: 2}
			b.Put([]byte("k"), nil)
			return b
		}},
		{"mixed ops binary keys", func() Batch {
			b := Batch{BaseSeq: base.MaxSeq - 10}
			b.Put([]byte{0x00, 0xff}, bytes.Repeat([]byte{0xab}, 300))
			b.Delete([]byte("gone"))
			b.Put(nil, []byte("empty-key"))
			b.Delete(nil)
			return b
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			want := tc.build()
			enc := want.Encode(nil)
			got, err := DecodeBatch(enc)
			if err != nil {
				t.Fatalf("DecodeBatch: %v", err)
			}
			if got.BaseSeq != want.BaseSeq {
				t.Fatalf("BaseSeq = %d, want %d", got.BaseSeq, want.BaseSeq)
			}
			if len(got.Ops) != len(want.Ops) {
				t.Fatalf("ops = %d, want %d", len(got.Ops), len(want.Ops))
			}
			for i := range want.Ops {
				w, g := want.Ops[i], got.Ops[i]
				if g.Kind != w.Kind || !bytes.Equal(g.UserKey, w.UserKey) || !bytes.Equal(g.Value, w.Value) {
					t.Fatalf("op %d = %+v, want %+v", i, g, w)
				}
			}
		})
	}
}

func TestDecodeBatchRejectsCorruption(t *testing.T) {
	b := Batch{BaseSeq: 42}
	b.Put([]byte("alpha"), []byte("one"))
	b.Delete([]byte("beta"))
	b.Put([]byte("gamma"), []byte("three"))
	enc := b.Encode(nil)

	for cut := 0; cut < len(enc); cut++ {
		if _, err := DecodeBatch(enc[:cut]); !errors.Is(err, ErrCorruptBatch) {
			t.Fatalf("prefix len %d: err = %v, want ErrCorruptBatch", cut, err)
		}
	}
	if _, err := DecodeBatch(append(append([]byte(nil), enc...), 0x00)); !errors.Is(err, ErrCorruptBatch) {
		t.Fatal("trailing byte must fail")
	}

	bad := append([]byte(nil), enc...)
	bad[9] = 0x7f
	if _, err := DecodeBatch(bad); !errors.Is(err, ErrCorruptBatch) {
		t.Fatalf("bad kind: err = %v, want ErrCorruptBatch", err)
	}
}

func TestDecodeBatchRejectsAbsurdCount(t *testing.T) {
	payload := binary.LittleEndian.AppendUint64(nil, 1)
	payload = binary.AppendUvarint(payload, 1<<40)
	payload = append(payload, bytes.Repeat([]byte{0}, 100)...)
	if _, err := DecodeBatch(payload); !errors.Is(err, ErrCorruptBatch) {
		t.Fatalf("absurd count = %v, want ErrCorruptBatch", err)
	}
}

func TestBatchThroughWAL(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWriter(Options{Dir: dir, Sync: SyncEveryRecord})
	if err != nil {
		t.Fatal(err)
	}
	var want []Batch
	for i := 0; i < 10; i++ {
		b := Batch{BaseSeq: uint64(i*3 + 1)}
		b.Put([]byte{byte('a' + i)}, []byte{byte('0' + i)})
		b.Delete([]byte{byte('a' + i), 'x'})
		want = append(want, b)
		mustAppend(t, w, b.Encode(nil))
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	var got []Batch
	err = Replay(dir, func(p []byte) error {
		b, err := DecodeBatch(p)
		if err != nil {
			return err
		}
		// The decoded batch aliases the replay buffer: deep-copy before
		// retaining past the callback.
		cp := Batch{BaseSeq: b.BaseSeq}
		for _, op := range b.Ops {
			cp.Ops = append(cp.Ops, Op{
				Kind:    op.Kind,
				UserKey: append([]byte(nil), op.UserKey...),
				Value:   append([]byte(nil), op.Value...),
			})
		}
		got = append(got, cp)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("replayed %d batches, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].BaseSeq != want[i].BaseSeq || len(got[i].Ops) != len(want[i].Ops) {
			t.Fatalf("batch %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}
