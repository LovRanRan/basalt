package sstable

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/LovRanRan/basalt/internal/base"
)

func benchTable(b *testing.B, n int) (*Reader, [][]byte) {
	b.Helper()
	var buf bytes.Buffer
	w := NewWriter(&buf, WriterOptions{})
	keys := make([][]byte, n)
	val := make([]byte, 100)
	for i := 0; i < n; i++ {
		keys[i] = []byte(fmt.Sprintf("key-%09d", i))
		if err := w.Add(base.AppendInternalKey(nil, keys[i], uint64(i+1), base.KindPut), val); err != nil {
			b.Fatal(err)
		}
	}
	if _, err := w.Finish(); err != nil {
		b.Fatal(err)
	}
	r, err := NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		b.Fatal(err)
	}
	return r, keys
}

func BenchmarkReaderGet(b *testing.B) {
	r, keys := benchTable(b, 100_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, ok, err := r.Get(keys[i%len(keys)], base.MaxSeq)
		if err != nil || !ok {
			b.Fatal(ok, err)
		}
	}
}

func BenchmarkIteratorNext(b *testing.B) {
	r, _ := benchTable(b, 100_000)
	it := r.NewIterator()
	it.First()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !it.Valid() {
			it.First()
		}
		it.Next()
	}
	if err := it.Error(); err != nil {
		b.Fatal(err)
	}
}
