package sstable

import "encoding/binary"

// leveldbHash is LevelDB's Hash function with its standard seed. It is part
// of the on-disk format — bloom bit positions depend on it — and is pinned
// by the golden test; never change it.
func leveldbHash(data []byte) uint32 {
	const (
		seed = 0xbc9f1d34
		m    = 0xc6a4a793
	)
	h := uint32(seed) ^ uint32(len(data))*m
	i := 0
	for ; i+4 <= len(data); i += 4 {
		h += binary.LittleEndian.Uint32(data[i:])
		h *= m
		h ^= h >> 16
	}
	switch len(data) - i {
	case 3:
		h += uint32(data[i+2]) << 16
		fallthrough
	case 2:
		h += uint32(data[i+1]) << 8
		fallthrough
	case 1:
		h += uint32(data[i])
		h *= m
		h ^= h >> 24
	}
	return h
}

// buildBloom builds one bloom filter over the whole table's user-key
// hashes: a bit array of roughly bitsPerKey bits per key, followed by one
// byte holding k, the probe count. Probes use double hashing, rotating the
// hash by 15 bits per step, LevelDB-style.
func buildBloom(hashes []uint32, bitsPerKey int) []byte {
	k := uint32(float64(bitsPerKey) * 0.69) // ≈ bitsPerKey · ln2, the optimal probe count
	if k < 1 {
		k = 1
	}
	if k > 30 {
		k = 30
	}
	bits := len(hashes) * bitsPerKey
	if bits < 64 {
		bits = 64
	}
	nBytes := (bits + 7) / 8
	bits = nBytes * 8
	filter := make([]byte, nBytes+1)
	filter[nBytes] = byte(k)
	for _, h := range hashes {
		delta := h>>17 | h<<15
		for j := uint32(0); j < k; j++ {
			bit := h % uint32(bits)
			filter[bit/8] |= 1 << (bit % 8)
			h += delta
		}
	}
	return filter
}

// bloomMayContain reports whether the filter possibly contains a key with
// the given hash. False negatives are impossible; a malformed filter reads
// as "maybe" so corruption can never hide a key.
func bloomMayContain(filter []byte, h uint32) bool {
	if len(filter) < 2 {
		return true
	}
	nBytes := len(filter) - 1
	bits := uint32(nBytes * 8)
	k := filter[nBytes]
	// bits==0 means the u32 wrapped on an absurdly large (corrupt) filter;
	// k>30 is reserved. Both read as "maybe" — and never divide by zero.
	if bits == 0 || k > 30 {
		return true
	}
	delta := h>>17 | h<<15
	for j := byte(0); j < k; j++ {
		bit := h % bits
		if filter[bit/8]&(1<<(bit%8)) == 0 {
			return false
		}
		h += delta
	}
	return true
}
