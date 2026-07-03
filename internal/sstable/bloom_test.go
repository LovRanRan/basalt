package sstable

import (
	"fmt"
	"testing"
)

func TestBloomNoFalseNegatives(t *testing.T) {
	const n = 100_000
	hashes := make([]uint32, 0, n)
	for i := 0; i < n; i++ {
		hashes = append(hashes, leveldbHash([]byte(fmt.Sprintf("key-%d", i))))
	}
	f := buildBloom(hashes, 10)
	for i := 0; i < n; i++ {
		if !bloomMayContain(f, leveldbHash([]byte(fmt.Sprintf("key-%d", i)))) {
			t.Fatalf("false negative for key-%d", i)
		}
	}
}

func TestBloomFalsePositiveRate(t *testing.T) {
	const n = 10_000
	hashes := make([]uint32, 0, n)
	for i := 0; i < n; i++ {
		hashes = append(hashes, leveldbHash([]byte(fmt.Sprintf("key-%d", i))))
	}
	f := buildBloom(hashes, 10)
	fp := 0
	for i := 0; i < n; i++ {
		if bloomMayContain(f, leveldbHash([]byte(fmt.Sprintf("absent-%d", i)))) {
			fp++
		}
	}
	rate := float64(fp) / n
	if rate > 0.025 {
		t.Fatalf("false positive rate %.4f exceeds 2.5%% at 10 bits/key", rate)
	}
}

func TestBloomSmallCases(t *testing.T) {
	empty := buildBloom(nil, 10)
	if len(empty) != 9 {
		t.Fatalf("empty filter length = %d, want 9 (64 bits + probe count)", len(empty))
	}
	if bloomMayContain(empty, leveldbHash([]byte("x"))) {
		t.Fatal("filter with no keys must not claim membership")
	}
	single := buildBloom([]uint32{leveldbHash([]byte("a"))}, 10)
	if !bloomMayContain(single, leveldbHash([]byte("a"))) {
		t.Fatal("false negative on a single-key filter")
	}
	if k := single[len(single)-1]; k != 6 {
		t.Fatalf("probe count at 10 bits/key = %d, want 6", k)
	}
}

func TestBloomMalformedReadsAsMaybe(t *testing.T) {
	if !bloomMayContain(nil, 42) {
		t.Fatal("nil filter must read as maybe — corruption can never hide a key")
	}
	if !bloomMayContain([]byte{0x00, 31}, 42) {
		t.Fatal("reserved probe count must read as maybe")
	}
}
