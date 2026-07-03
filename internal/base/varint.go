package base

import (
	"encoding/binary"
	"errors"
)

var ErrCorruptFraming = errors.New("base: corrupt length-prefixed value")

// AppendLengthPrefixed appends val to dst framed by its uvarint length.
func AppendLengthPrefixed(dst, val []byte) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(val)))
	return append(dst, val...)
}

// DecodeLengthPrefixed reads one length-prefixed value from src, returning
// the value and the unread remainder. The value aliases src, capped so
// appends through it cannot reach the remainder.
func DecodeLengthPrefixed(src []byte) (val, rest []byte, err error) {
	n, w := binary.Uvarint(src)
	if w <= 0 {
		return nil, nil, ErrCorruptFraming
	}
	if n > uint64(len(src)-w) {
		return nil, nil, ErrCorruptFraming
	}
	end := w + int(n)
	return src[w:end:end], src[end:], nil
}
