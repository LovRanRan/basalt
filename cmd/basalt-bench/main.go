// Command basalt-bench drives the engine through db_bench-style workloads
// and reports throughput, latency percentiles, and background-work counters
// so write amplification is visible.
package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	basalt "github.com/LovRanRan/basalt"
)

var (
	dir         = flag.String("dir", "", "database directory (default: a temp dir)")
	n           = flag.Int("n", 200_000, "operations per workload")
	valueSize   = flag.Int("value-size", 100, "value bytes")
	concurrency = flag.Int("concurrency", 4, "reader goroutines for read workloads")
	workloads   = flag.String("workloads", "fillseq,fillrandom,readrandom,readwhilewriting,scan", "comma-separated workloads")
	sync_       = flag.Bool("sync", false, "fsync the WAL on every write")
	smoke       = flag.Bool("smoke", false, "tiny run for CI: n=2000")
)

func key(i int) []byte { return fmt.Appendf(nil, "key-%012d", i) }

type recorder struct {
	mu   sync.Mutex
	lats []time.Duration
}

func (r *recorder) record(d time.Duration) {
	r.mu.Lock()
	r.lats = append(r.lats, d)
	r.mu.Unlock()
}

func (r *recorder) report(name string, total time.Duration) {
	sort.Slice(r.lats, func(i, j int) bool { return r.lats[i] < r.lats[j] })
	pct := func(p float64) time.Duration {
		if len(r.lats) == 0 {
			return 0
		}
		i := int(p * float64(len(r.lats)-1))
		return r.lats[i]
	}
	ops := float64(len(r.lats)) / total.Seconds()
	fmt.Printf("%-17s %8d ops in %7.2fs  %9.0f ops/s  p50=%-9s p95=%-9s p99=%s\n",
		name, len(r.lats), total.Seconds(), ops, pct(0.50), pct(0.95), pct(0.99))
}

func main() {
	flag.Parse()
	if *smoke {
		*n = 2000
	}
	d := *dir
	if d == "" {
		var err error
		if d, err = os.MkdirTemp("", "basalt-bench-*"); err != nil {
			fatal(err)
		}
		defer func() { _ = os.RemoveAll(d) }()
	}
	db, err := basalt.Open(filepath.Join(d, "db"), basalt.Options{DisableWALSync: !*sync_})
	if err != nil {
		fatal(err)
	}
	defer func() { _ = db.Close() }()

	value := make([]byte, *valueSize)
	for i := range value {
		value[i] = byte('a' + i%26)
	}
	fmt.Printf("basalt-bench: n=%d value=%dB sync=%v concurrency=%d\n", *n, *valueSize, *sync_, *concurrency)

	filled := false
	for _, w := range strings.Split(*workloads, ",") {
		switch strings.TrimSpace(w) {
		case "fillseq":
			rec := &recorder{}
			start := time.Now()
			for i := 0; i < *n; i++ {
				t0 := time.Now()
				if err := db.Put(key(i), value); err != nil {
					fatal(err)
				}
				rec.record(time.Since(t0))
			}
			rec.report("fillseq", time.Since(start))
			filled = true
		case "fillrandom":
			rng := rand.New(rand.NewSource(1))
			rec := &recorder{}
			start := time.Now()
			for i := 0; i < *n; i++ {
				t0 := time.Now()
				if err := db.Put(key(rng.Intn(*n)), value); err != nil {
					fatal(err)
				}
				rec.record(time.Since(t0))
			}
			rec.report("fillrandom", time.Since(start))
			filled = true
		case "readrandom":
			ensureFilled(db, &filled, *n, value)
			rec := &recorder{}
			start := time.Now()
			var wg sync.WaitGroup
			per := *n / *concurrency
			for g := 0; g < *concurrency; g++ {
				wg.Add(1)
				go func(seed int64) {
					defer wg.Done()
					rng := rand.New(rand.NewSource(seed))
					for i := 0; i < per; i++ {
						t0 := time.Now()
						if _, err := db.Get(key(rng.Intn(*n))); err != nil {
							fatal(fmt.Errorf("readrandom: %w", err))
						}
						rec.record(time.Since(t0))
					}
				}(int64(g))
			}
			wg.Wait()
			rec.report("readrandom", time.Since(start))
		case "readwhilewriting":
			ensureFilled(db, &filled, *n, value)
			rec := &recorder{}
			stop := make(chan struct{})
			go func() {
				rng := rand.New(rand.NewSource(7))
				for {
					select {
					case <-stop:
						return
					default:
					}
					if err := db.Put(key(rng.Intn(*n)), value); err != nil {
						fatal(err)
					}
				}
			}()
			start := time.Now()
			var wg sync.WaitGroup
			per := *n / *concurrency
			for g := 0; g < *concurrency; g++ {
				wg.Add(1)
				go func(seed int64) {
					defer wg.Done()
					rng := rand.New(rand.NewSource(seed))
					for i := 0; i < per; i++ {
						t0 := time.Now()
						if _, err := db.Get(key(rng.Intn(*n))); err != nil {
							fatal(fmt.Errorf("readwhilewriting: %w", err))
						}
						rec.record(time.Since(t0))
					}
				}(int64(g))
			}
			wg.Wait()
			total := time.Since(start)
			close(stop)
			rec.report("readwhilewriting", total)
		case "scan":
			ensureFilled(db, &filled, *n, value)
			rec := &recorder{}
			scans := 10
			if *smoke {
				scans = 3
			}
			keys := 0
			start := time.Now()
			for s := 0; s < scans; s++ {
				t0 := time.Now()
				it := db.Scan(nil, nil)
				for ; it.Valid(); it.Next() {
					keys++
				}
				if err := it.Error(); err != nil {
					fatal(err)
				}
				it.Close()
				rec.record(time.Since(t0))
			}
			total := time.Since(start)
			rec.report("scan", total)
			fmt.Printf("%-17s %8.0f keys/s over %d full scans\n", "", float64(keys)/total.Seconds(), scans)
		default:
			fatal(fmt.Errorf("unknown workload %q", w))
		}
	}

	m := db.Metrics()
	fmt.Printf("engine: flushes=%d compactions=%d flushed=%.1fMB compacted=%.1fMB\n",
		m.Flushes, m.Compactions, float64(m.FlushBytes)/1e6, float64(m.CompactionBytes)/1e6)
}

func ensureFilled(db *basalt.DB, filled *bool, n int, value []byte) {
	if *filled {
		return
	}
	for i := 0; i < n; i++ {
		if err := db.Put(key(i), value); err != nil {
			fatal(err)
		}
	}
	*filled = true
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "basalt-bench:", err)
	os.Exit(1)
}
