package memtable_test

import (
	"fmt"
	"testing"

	"github.com/LovRanRan/basalt/internal/base"
	"github.com/LovRanRan/basalt/internal/memtable"
)

func BenchmarkAdd(b *testing.B) {
	m := memtable.New()
	val := make([]byte, 100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Add(fmt.Appendf(nil, "key-%09d", i), uint64(i+1), base.KindPut, val)
	}
}

func BenchmarkGet(b *testing.B) {
	m := memtable.New()
	const n = 100_000
	for i := 0; i < n; i++ {
		m.Add(fmt.Appendf(nil, "key-%09d", i), uint64(i+1), base.KindPut, []byte("v"))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, ok := m.Get(fmt.Appendf(nil, "key-%09d", i%n), base.MaxSeq); !ok {
			b.Fatal("missing key")
		}
	}
}
