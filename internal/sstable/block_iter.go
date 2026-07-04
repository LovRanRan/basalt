package sstable

import (
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/LovRanRan/basalt/internal/base"
)

// parsedBlock is a validated block: its entry region and decoded restart
// offsets. Validation happens once at load so iteration can trust offsets.
type parsedBlock struct {
	entries  []byte
	restarts []uint32
}

// parseBlock validates a block's restart array: count in range, offsets
// strictly increasing, every offset inside the entry region (an empty block
// encodes one restart at offset 0 of an empty region).
func parseBlock(contents []byte, region string) (parsedBlock, error) {
	if len(contents) < 8 {
		return parsedBlock{}, corruptf("%s: block too short", region)
	}
	// All arithmetic in uint64: int is 32 bits on some platforms and a
	// crafted count or offset >= 2^31 must not go negative past a check.
	n64 := uint64(binary.LittleEndian.Uint32(contents[len(contents)-4:]))
	if n64 == 0 || 4+4*n64 > uint64(len(contents)) {
		return parsedBlock{}, corruptf("%s: restart count %d out of range", region, n64)
	}
	n := int(n64)
	entriesEnd := len(contents) - 4 - 4*n
	restarts := make([]uint32, n)
	for i := range restarts {
		restarts[i] = binary.LittleEndian.Uint32(contents[entriesEnd+4*i:])
		if uint64(restarts[i]) > uint64(entriesEnd) || (entriesEnd > 0 && uint64(restarts[i]) >= uint64(entriesEnd)) {
			return parsedBlock{}, corruptf("%s: restart offset %d past entries", region, restarts[i])
		}
		if i > 0 && restarts[i] <= restarts[i-1] {
			return parsedBlock{}, corruptf("%s: restart offsets not increasing", region)
		}
	}
	return parsedBlock{entries: contents[:entriesEnd], restarts: restarts}, nil
}

// blockIter iterates one parsed block. Key returns a buffer reused across
// positioning calls; Value aliases the block buffer.
type blockIter struct {
	b          parsedBlock
	region     string
	nextOffset int
	key        []byte
	value      []byte
	lastShared uint64
	valid      bool
	err        error
}

func newBlockIter(b parsedBlock, region string) *blockIter {
	return &blockIter{b: b, region: region}
}

func (it *blockIter) fail(err error) {
	if it.err == nil {
		it.err = err
	}
	it.valid = false
}

// decodeNext decodes the entry at nextOffset, extending the current key by
// the entry's shared prefix; the caller guarantees the prefix state matches
// (sequential decode from a restart point).
func (it *blockIter) decodeNext() {
	off := it.nextOffset
	if off >= len(it.b.entries) {
		it.valid = false
		return
	}
	rest := it.b.entries[off:]
	shared, n1 := binary.Uvarint(rest)
	if n1 <= 0 {
		it.fail(corruptf("%s: bad entry header at %d", it.region, off))
		return
	}
	rest = rest[n1:]
	unshared, n2 := binary.Uvarint(rest)
	if n2 <= 0 {
		it.fail(corruptf("%s: bad entry header at %d", it.region, off))
		return
	}
	rest = rest[n2:]
	vlen, n3 := binary.Uvarint(rest)
	if n3 <= 0 {
		it.fail(corruptf("%s: bad entry header at %d", it.region, off))
		return
	}
	rest = rest[n3:]
	if shared > uint64(len(it.key)) {
		it.fail(corruptf("%s: shared prefix %d exceeds previous key at %d", it.region, shared, off))
		return
	}
	// Two separate comparisons: a sum of attacker-controlled uvarints can
	// wrap uint64 and slip past a combined check.
	if unshared > uint64(len(rest)) || vlen > uint64(len(rest))-unshared {
		it.fail(corruptf("%s: entry at %d overruns block", it.region, off))
		return
	}
	it.key = append(it.key[:shared], rest[:unshared]...)
	if len(it.key) < base.TrailerLen {
		it.fail(corruptf("%s: key shorter than trailer at %d", it.region, off))
		return
	}
	it.value = rest[unshared : unshared+vlen : unshared+vlen]
	it.lastShared = shared
	it.nextOffset = off + n1 + n2 + n3 + int(unshared) + int(vlen)
	it.valid = true
}

// seekRestart positions decoding at restart i; restart entries carry full
// keys, so the prefix state resets.
func (it *blockIter) seekRestart(i int) {
	it.key = it.key[:0]
	it.nextOffset = int(it.b.restarts[i])
	it.decodeNext()
}

func (it *blockIter) first() {
	it.key = it.key[:0]
	it.nextOffset = 0
	it.decodeNext()
}

// restartKey decodes the full key at restart i without moving the iterator.
func (it *blockIter) restartKey(i int) ([]byte, error) {
	off := int(it.b.restarts[i])
	rest := it.b.entries[off:]
	shared, n1 := binary.Uvarint(rest)
	if n1 <= 0 || shared != 0 {
		return nil, corruptf("%s: restart %d is not a full key", it.region, i)
	}
	rest = rest[n1:]
	unshared, n2 := binary.Uvarint(rest)
	if n2 <= 0 {
		return nil, corruptf("%s: bad restart entry %d", it.region, i)
	}
	rest = rest[n2:]
	_, n3 := binary.Uvarint(rest)
	if n3 <= 0 {
		return nil, corruptf("%s: bad restart entry %d", it.region, i)
	}
	rest = rest[n3:]
	// The key feeds base.Compare, which requires a full trailer.
	if unshared < uint64(base.TrailerLen) || unshared > uint64(len(rest)) {
		return nil, corruptf("%s: restart entry %d key out of range", it.region, i)
	}
	return rest[:unshared], nil
}

// seekGE positions at the first entry whose key is >= target: binary search
// for the last restart with a key < target, then scan forward.
func (it *blockIter) seekGE(target []byte) {
	if len(it.b.entries) == 0 {
		it.valid = false
		return
	}
	var searchErr error
	i := sort.Search(len(it.b.restarts), func(i int) bool {
		if searchErr != nil {
			return true
		}
		k, err := it.restartKey(i)
		if err != nil {
			searchErr = err
			return true
		}
		return base.Compare(k, target) >= 0
	})
	if searchErr != nil {
		it.fail(searchErr)
		return
	}
	start := 0
	if i > 0 {
		start = i - 1
	}
	it.seekRestart(start)
	for it.valid && base.Compare(it.key, target) < 0 {
		it.next()
	}
}

func (it *blockIter) next() {
	if !it.valid {
		panic(fmt.Sprintf("sstable: next on invalid block iterator (%s)", it.region))
	}
	it.decodeNext()
}
