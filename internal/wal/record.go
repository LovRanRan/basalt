package wal

import (
	"encoding/binary"
	"hash/crc32"
	"math"
)

// Record framing: crc32c (4 bytes, computed over length and payload), then
// payload length (4 bytes), then the payload, all little-endian. Covering
// the length field with the checksum makes any single corrupted byte in a
// record detectable — a flipped length cannot silently reframe the stream.
// The manifest (P1.6) reuses this framing, which is why it is exported.
const RecordHeaderLen = 8

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// AppendRecord appends one framed record to dst. It panics if the payload
// exceeds the u32 length field: silently truncating the length would write
// a permanently unparseable frame, and the bound is the caller's logic
// error, not runtime input.
func AppendRecord(dst, payload []byte) []byte {
	if uint64(len(payload)) > math.MaxUint32 {
		panic("wal: record payload exceeds u32 length field")
	}
	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(payload)))
	c := crc32.Checksum(lenBuf[:], castagnoli)
	c = crc32.Update(c, castagnoli, payload)
	dst = binary.LittleEndian.AppendUint32(dst, c)
	dst = append(dst, lenBuf[:]...)
	return append(dst, payload...)
}

// NextRecord parses one record from buf. ok=false means buf begins with a
// torn or corrupt record — short header, length past the end of buf, or
// checksum mismatch; the caller decides whether that is an expected tail or
// real corruption. payload aliases buf, capacity-capped.
func NextRecord(buf []byte) (payload, rest []byte, ok bool) {
	if len(buf) < RecordHeaderLen {
		return nil, nil, false
	}
	wantCRC := binary.LittleEndian.Uint32(buf[:4])
	length := binary.LittleEndian.Uint32(buf[4:8])
	if uint64(length) > uint64(len(buf)-RecordHeaderLen) {
		return nil, nil, false
	}
	end := RecordHeaderLen + int(length)
	c := crc32.Checksum(buf[4:8], castagnoli)
	c = crc32.Update(c, castagnoli, buf[RecordHeaderLen:end])
	if c != wantCRC {
		return nil, nil, false
	}
	return buf[RecordHeaderLen:end:end], buf[end:], true
}

// ScanRecords walks the records in buf, invoking fn (when non-nil) with
// each payload, and returns the length of the clean prefix: the offset of
// the first torn or corrupt record, or len(buf) when every byte parses. An
// error from fn aborts the scan and is returned as-is.
func ScanRecords(buf []byte, fn func(payload []byte) error) (cleanLen int, err error) {
	rest := buf
	for len(rest) > 0 {
		payload, next, ok := NextRecord(rest)
		if !ok {
			break
		}
		if fn != nil {
			if err := fn(payload); err != nil {
				return len(buf) - len(rest), err
			}
		}
		rest = next
	}
	return len(buf) - len(rest), nil
}
