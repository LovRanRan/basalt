package basalt

import (
	"fmt"
	"testing"

	"github.com/LovRanRan/basalt/internal/base"
	"github.com/LovRanRan/basalt/internal/memtable"
)

func BenchmarkMergingIterNext(b *testing.B) {
	const sources = 4
	var children []internalIterator
	seq := uint64(1)
	for s := 0; s < sources; s++ {
		m := memtable.New()
		for i := 0; i < 25_000; i++ {
			m.Add(fmt.Appendf(nil, "key-%d-%06d", s, i), seq, base.KindPut, []byte("v"))
			seq++
		}
		children = append(children, m.NewIterator())
	}
	mi := newMergingIter(children)
	mi.First()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !mi.Valid() {
			mi.First()
		}
		mi.Next()
	}
}
