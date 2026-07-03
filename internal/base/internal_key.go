package base

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
)

// Kind tags an internal key with the operation that produced it.
type Kind uint8

const (
	KindDelete Kind = 0
	KindPut    Kind = 1

	maxKind = KindPut
)

const (
	// TrailerLen is the length of the (seq, kind) trailer that terminates
	// every encoded internal key.
	TrailerLen = 8

	// MaxSeq is the largest representable sequence number; the trailer
	// packs seq into 56 bits alongside the 8-bit kind.
	MaxSeq = uint64(1)<<56 - 1
)

var (
	ErrKeyTooShort = errors.New("base: encoded internal key shorter than trailer")
	ErrInvalidKind = errors.New("base: invalid internal key kind")
)

// InternalKey is the decoded form of a versioned key. Ordering — and
// therefore every on-disk and in-memory structure — is defined on the
// encoded form via Compare.
type InternalKey struct {
	UserKey []byte
	Seq     uint64
	Kind    Kind
}

// AppendInternalKey appends the encoded form of (userKey, seq, kind) to dst.
// It panics if seq exceeds MaxSeq or kind is invalid: both are
// engine-assigned, so bad values are logic errors, not input corruption.
func AppendInternalKey(dst, userKey []byte, seq uint64, kind Kind) []byte {
	if seq > MaxSeq {
		panic(fmt.Sprintf("base: seq %d exceeds MaxSeq", seq))
	}
	if kind > maxKind {
		panic(fmt.Sprintf("base: invalid kind %d", kind))
	}
	dst = append(dst, userKey...)
	var trailer [TrailerLen]byte
	binary.LittleEndian.PutUint64(trailer[:], seq<<8|uint64(kind))
	return append(dst, trailer[:]...)
}

// AppendSeekKey appends the key that SeekGE-positions an iterator at the
// newest version of userKey visible at seq. Kinds order descending within a
// sequence number, so the seek key must carry the maximum kind — built with
// any lower kind, SeekGE would skip same-seq entries of higher kind.
func AppendSeekKey(dst, userKey []byte, seq uint64) []byte {
	return AppendInternalKey(dst, userKey, seq, maxKind)
}

// DecodeInternalKey splits an encoded internal key into its parts. The
// returned UserKey aliases encoded's backing array.
func DecodeInternalKey(encoded []byte) (InternalKey, error) {
	if len(encoded) < TrailerLen {
		return InternalKey{}, ErrKeyTooShort
	}
	trailer := binary.LittleEndian.Uint64(encoded[len(encoded)-TrailerLen:])
	kind := Kind(trailer & 0xff)
	if kind > maxKind {
		return InternalKey{}, fmt.Errorf("%w: %d", ErrInvalidKind, kind)
	}
	return InternalKey{
		UserKey: UserKey(encoded),
		Seq:     trailer >> 8,
		Kind:    kind,
	}, nil
}

// UserKey returns the user-key prefix of an encoded internal key, capped so
// appends through it cannot clobber the trailer. It panics on inputs shorter
// than the trailer: callers must only pass keys produced by AppendInternalKey.
func UserKey(encoded []byte) []byte {
	n := len(encoded) - TrailerLen
	return encoded[:n:n]
}

// Compare orders encoded internal keys by user key ascending, then sequence
// number descending, then kind descending, so the newest version of a user
// key is encountered first in any forward iteration.
//
// Both arguments must be valid encoded internal keys (len >= TrailerLen);
// keys parsed from untrusted bytes (disk blocks) must be length-validated,
// e.g. via DecodeInternalKey, before they reach Compare.
func Compare(a, b []byte) int {
	if c := bytes.Compare(UserKey(a), UserKey(b)); c != 0 {
		return c
	}
	ta := binary.LittleEndian.Uint64(a[len(a)-TrailerLen:])
	tb := binary.LittleEndian.Uint64(b[len(b)-TrailerLen:])
	switch {
	case ta > tb:
		return -1
	case ta < tb:
		return 1
	default:
		return 0
	}
}
