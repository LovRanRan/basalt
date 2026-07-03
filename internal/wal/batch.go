package wal

import (
	"encoding/binary"
	"errors"

	"github.com/LovRanRan/basalt/internal/base"
)

// Op is one write in a batch. Value is nil for deletes.
type Op struct {
	Kind    base.Kind
	UserKey []byte
	Value   []byte
}

// Batch is the payload the engine logs per write: the puts and deletes
// applied under consecutive sequence numbers BaseSeq..BaseSeq+len(Ops)-1,
// one seqno per op, in order.
type Batch struct {
	BaseSeq uint64
	Ops     []Op
}

var ErrCorruptBatch = errors.New("wal: corrupt batch payload")

func (b *Batch) Put(userKey, value []byte) {
	b.Ops = append(b.Ops, Op{Kind: base.KindPut, UserKey: userKey, Value: value})
}

func (b *Batch) Delete(userKey []byte) {
	b.Ops = append(b.Ops, Op{Kind: base.KindDelete, UserKey: userKey})
}

// Encode appends the batch encoding to dst: fixed 8-byte BaseSeq, uvarint op
// count, then per op a kind byte, the length-prefixed user key, and — for
// puts only — the length-prefixed value.
func (b *Batch) Encode(dst []byte) []byte {
	dst = binary.LittleEndian.AppendUint64(dst, b.BaseSeq)
	dst = binary.AppendUvarint(dst, uint64(len(b.Ops)))
	for _, op := range b.Ops {
		dst = append(dst, byte(op.Kind))
		dst = base.AppendLengthPrefixed(dst, op.UserKey)
		if op.Kind == base.KindPut {
			dst = base.AppendLengthPrefixed(dst, op.Value)
		}
	}
	return dst
}

// DecodeBatch parses an encoded batch. Decoded keys and values alias
// payload. Trailing bytes, an op count that outruns the payload, or an
// unknown kind all fail: a batch decodes exactly or not at all.
func DecodeBatch(payload []byte) (Batch, error) {
	if len(payload) < 8 {
		return Batch{}, ErrCorruptBatch
	}
	b := Batch{BaseSeq: binary.LittleEndian.Uint64(payload[:8])}
	rest := payload[8:]
	count, w := binary.Uvarint(rest)
	if w <= 0 {
		return Batch{}, ErrCorruptBatch
	}
	rest = rest[w:]
	// Every op takes at least two bytes (kind + empty-key length), so an
	// absurd count fails here; the allocation is additionally capped so a
	// corrupt count can never size a huge slice up front.
	if count > uint64(len(rest))/2 {
		return Batch{}, ErrCorruptBatch
	}
	b.Ops = make([]Op, 0, min(count, 4096))
	for i := uint64(0); i < count; i++ {
		if len(rest) == 0 {
			return Batch{}, ErrCorruptBatch
		}
		op := Op{Kind: base.Kind(rest[0])}
		if op.Kind != base.KindPut && op.Kind != base.KindDelete {
			return Batch{}, ErrCorruptBatch
		}
		rest = rest[1:]
		var err error
		op.UserKey, rest, err = base.DecodeLengthPrefixed(rest)
		if err != nil {
			return Batch{}, ErrCorruptBatch
		}
		if op.Kind == base.KindPut {
			op.Value, rest, err = base.DecodeLengthPrefixed(rest)
			if err != nil {
				return Batch{}, ErrCorruptBatch
			}
		}
		b.Ops = append(b.Ops, op)
	}
	if len(rest) != 0 {
		return Batch{}, ErrCorruptBatch
	}
	return b, nil
}
