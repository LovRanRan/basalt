// Command basalt-ycsb is a YCSB-style mixed-workload benchmark driver. It
// loads a keyspace then runs the standard workload mixes (A-F) with a
// Zipfian request distribution, reporting per-operation latency histograms
// (p50/p95/p99/p99.9/max) and throughput. It can drive the in-process storage
// engine (-engine) or a running distributed cluster (-cluster).
package main

import (
	"context"
	"flag"
	"fmt"
	"hash/fnv"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	basalt "github.com/LovRanRan/basalt"
	"github.com/LovRanRan/basalt/cluster"
)

func main() {
	cfg := config{}
	flag.StringVar(&cfg.workload, "workload", "a", "YCSB workload: a,b,c,d,e,f")
	flag.IntVar(&cfg.records, "records", 100_000, "records to load")
	flag.IntVar(&cfg.ops, "ops", 100_000, "operations in the run phase")
	flag.IntVar(&cfg.threads, "threads", 8, "concurrent client threads")
	flag.IntVar(&cfg.valueSize, "value-size", 100, "value bytes")
	flag.Float64Var(&cfg.theta, "zipf-theta", 0.99, "Zipfian skew (0=uniform-ish, 0.99=YCSB default)")
	flag.Int64Var(&cfg.seed, "seed", 1, "PRNG seed")
	engineDir := flag.String("engine", "", "run against an in-process engine at this dir (empty temp if '-')")
	clusterAddrs := flag.String("cluster", "", "run against a cluster: id=host:port,id=host:port,...")
	smoke := flag.Bool("smoke", false, "tiny run for CI")
	flag.Parse()

	if *smoke {
		cfg.records, cfg.ops, cfg.threads = 2000, 2000, 4
	}
	if err := cfg.spec(); err != nil {
		fatal(err)
	}

	var st store
	var closeSt func()
	switch {
	case *clusterAddrs != "":
		addrs, err := parseCluster(*clusterAddrs)
		if err != nil {
			fatal(err)
		}
		c, err := cluster.NewClient(addrs)
		if err != nil {
			fatal(err)
		}
		st, closeSt = &clusterStore{c: c}, c.Close
		fmt.Printf("basalt-ycsb: target=cluster(%d nodes)\n", len(addrs))
	default:
		dir := *engineDir
		if dir == "" || dir == "-" {
			d, err := os.MkdirTemp("", "basalt-ycsb-*")
			if err != nil {
				fatal(err)
			}
			dir = d
			defer func() { _ = os.RemoveAll(d) }()
		}
		db, err := basalt.Open(filepath.Join(dir, "db"), basalt.Options{DisableWALSync: true})
		if err != nil {
			fatal(err)
		}
		st, closeSt = &engineStore{db: db}, func() { _ = db.Close() }
		fmt.Printf("basalt-ycsb: target=engine(%s)\n", dir)
	}
	defer closeSt()

	res, err := run(cfg, st)
	if err != nil {
		fatal(err)
	}
	res.print(cfg)
}

// config is a parsed benchmark configuration.
type config struct {
	workload             string
	records, ops         int
	threads, valueSize   int
	theta                float64
	seed                 int64
	read, update, insert float64 // run-phase op proportions (+ scan/rmw below)
	scan, rmw            float64
	dist                 distKind
}

type distKind int

const (
	distZipfian distKind = iota
	distLatest
	distUniform
)

// spec fills the op proportions and distribution for the named workload.
func (c *config) spec() error {
	switch strings.ToLower(c.workload) {
	case "a": // update-heavy: 50/50 read/update, zipfian
		c.read, c.update, c.dist = 0.5, 0.5, distZipfian
	case "b": // read-heavy: 95/5 read/update, zipfian
		c.read, c.update, c.dist = 0.95, 0.05, distZipfian
	case "c": // read-only, zipfian
		c.read, c.dist = 1.0, distZipfian
	case "d": // read-latest: 95/5 read/insert, latest distribution
		c.read, c.insert, c.dist = 0.95, 0.05, distLatest
	case "e": // scan-heavy: 95/5 scan/insert, zipfian (engine only)
		c.scan, c.insert, c.dist = 0.95, 0.05, distZipfian
	case "f": // read-modify-write: 50 read + 50 rmw, zipfian
		c.read, c.rmw, c.dist = 0.5, 0.5, distZipfian
	default:
		return fmt.Errorf("unknown workload %q (want a-f)", c.workload)
	}
	return nil
}

// store is the minimal surface a YCSB run needs; engine and cluster implement it.
type store interface {
	insert(key string, val []byte) error
	read(key string) error
	scan(startKey string, count int) error
}

type engineStore struct{ db *basalt.DB }

func (s *engineStore) insert(key string, val []byte) error { return s.db.Put([]byte(key), val) }
func (s *engineStore) read(key string) error {
	_, err := s.db.Get([]byte(key))
	if err == basalt.ErrNotFound {
		return nil
	}
	return err
}
func (s *engineStore) scan(startKey string, count int) error {
	it := s.db.Scan([]byte(startKey), nil)
	defer it.Close()
	for i := 0; i < count && it.Valid(); i++ {
		_ = it.Key()
		_ = it.Value()
		it.Next()
	}
	return it.Error()
}

type clusterStore struct{ c *cluster.Client }

func (s *clusterStore) insert(key string, val []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.c.Put(ctx, []byte(key), val)
}
func (s *clusterStore) read(key string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _, err := s.c.Get(ctx, []byte(key))
	return err
}
func (s *clusterStore) scan(string, int) error {
	return fmt.Errorf("workload E (scan) is engine-only; the cluster client has no Scan")
}

// results holds per-operation-type latency histograms plus the run duration.
type results struct {
	hists map[string]*hist
	total time.Duration
	loadT time.Duration
	loadN int
}

// run executes the load phase then the run phase, returning the histograms.
func run(cfg config, st store) (*results, error) {
	val := make([]byte, cfg.valueSize)
	for i := range val {
		val[i] = byte('a' + i%26)
	}

	// --- load phase: insert `records` keys sequentially-hashed ---
	loadStart := time.Now()
	loaded, err := loadPhase(cfg, st, val)
	if err != nil {
		return nil, fmt.Errorf("load: %w", err)
	}
	loadDur := time.Since(loadStart)

	// --- run phase: `ops` operations per the workload mix ---
	res := &results{hists: map[string]*hist{}, loadT: loadDur, loadN: loaded}
	for _, name := range []string{"READ", "UPDATE", "INSERT", "SCAN", "RMW"} {
		res.hists[name] = &hist{}
	}
	var mu sync.Mutex
	merge := func(local map[string]*hist) {
		mu.Lock()
		defer mu.Unlock()
		for name, h := range local {
			res.hists[name].merge(h)
		}
	}

	// A shared, atomically-advancing insert counter models YCSB's monotonically
	// growing keyspace for insert/latest ops.
	inserted := &counter{v: int64(loaded)}
	zipf := newZipf(uint64(max(loaded, 1)), cfg.theta)

	runStart := time.Now()
	var wg sync.WaitGroup
	per := cfg.ops / cfg.threads
	for t := 0; t < cfg.threads; t++ {
		wg.Add(1)
		n := per
		if t == cfg.threads-1 {
			n += cfg.ops - per*cfg.threads // remainder to the last thread
		}
		go func(seed int64, n int) {
			defer wg.Done()
			local, werr := worker(cfg, st, val, zipf, inserted, seed, n)
			if werr != nil {
				mu.Lock()
				if err == nil {
					err = werr
				}
				mu.Unlock()
			}
			merge(local)
		}(cfg.seed+int64(t)*2654435761, n)
	}
	wg.Wait()
	res.total = time.Since(runStart)
	return res, err
}

func loadPhase(cfg config, st store, val []byte) (int, error) {
	var wg sync.WaitGroup
	per := cfg.records / cfg.threads
	errc := make(chan error, cfg.threads)
	for t := 0; t < cfg.threads; t++ {
		lo := t * per
		hi := lo + per
		if t == cfg.threads-1 {
			hi = cfg.records
		}
		wg.Add(1)
		go func(lo, hi int) {
			defer wg.Done()
			for i := lo; i < hi; i++ {
				if err := st.insert(userKey(uint64(i)), val); err != nil {
					errc <- err
					return
				}
			}
		}(lo, hi)
	}
	wg.Wait()
	close(errc)
	if err := <-errc; err != nil {
		return 0, err
	}
	return cfg.records, nil
}

// worker runs n operations drawn from the workload mix and records latencies.
func worker(cfg config, st store, val []byte, z *zipf, inserted *counter, seed int64, n int) (map[string]*hist, error) {
	rng := rand.New(rand.NewSource(seed))
	h := map[string]*hist{"READ": {}, "UPDATE": {}, "INSERT": {}, "SCAN": {}, "RMW": {}}
	timed := func(name string, fn func() error) error {
		t0 := time.Now()
		err := fn()
		h[name].record(time.Since(t0))
		return err
	}
	for i := 0; i < n; i++ {
		r := rng.Float64()
		switch {
		case r < cfg.read:
			k := userKey(z.pick(rng, cfg.dist, inserted.load()))
			if err := timed("READ", func() error { return st.read(k) }); err != nil {
				return h, err
			}
		case r < cfg.read+cfg.update:
			k := userKey(z.pick(rng, cfg.dist, inserted.load()))
			if err := timed("UPDATE", func() error { return st.insert(k, val) }); err != nil {
				return h, err
			}
		case r < cfg.read+cfg.update+cfg.rmw:
			k := userKey(z.pick(rng, cfg.dist, inserted.load()))
			if err := timed("RMW", func() error {
				if err := st.read(k); err != nil {
					return err
				}
				return st.insert(k, val)
			}); err != nil {
				return h, err
			}
		case r < cfg.read+cfg.update+cfg.rmw+cfg.scan:
			k := userKey(z.pick(rng, cfg.dist, inserted.load()))
			cnt := 1 + rng.Intn(100)
			if err := timed("SCAN", func() error { return st.scan(k, cnt) }); err != nil {
				return h, err
			}
		default: // insert
			id := inserted.add(1) - 1
			k := userKey(uint64(id))
			if err := timed("INSERT", func() error { return st.insert(k, val) }); err != nil {
				return h, err
			}
		}
	}
	return h, nil
}

// userKey hashes an index into a scrambled key so Zipfian-hot indices are not
// clustered in the keyspace, matching YCSB's ScrambledZipfian.
func userKey(i uint64) string {
	h := fnv.New64a()
	var b [8]byte
	for j := 0; j < 8; j++ {
		b[j] = byte(i >> (8 * j))
	}
	_, _ = h.Write(b[:])
	return "user" + strconv.FormatUint(h.Sum64(), 10)
}

func parseCluster(spec string) (map[uint64]string, error) {
	out := map[uint64]string{}
	for _, part := range strings.Split(spec, ",") {
		id, addr, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			return nil, fmt.Errorf("bad -cluster entry %q (want id=host:port)", part)
		}
		n, err := strconv.ParseUint(id, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("bad node id %q: %w", id, err)
		}
		out[n] = addr
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("empty -cluster spec")
	}
	return out, nil
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "basalt-ycsb:", err)
	os.Exit(1)
}

func (r *results) print(cfg config) {
	fmt.Printf("workload=%s records=%d ops=%d threads=%d value=%dB\n",
		strings.ToLower(cfg.workload), cfg.records, cfg.ops, cfg.threads, cfg.valueSize)
	if r.loadT > 0 {
		fmt.Printf("load: %d records in %.2fs (%.0f ops/s)\n",
			r.loadN, r.loadT.Seconds(), float64(r.loadN)/r.loadT.Seconds())
	}
	total := 0
	for _, name := range []string{"READ", "UPDATE", "INSERT", "SCAN", "RMW"} {
		if h := r.hists[name]; h.count() > 0 {
			h.report(name)
			total += h.count()
		}
	}
	if r.total > 0 {
		fmt.Printf("[overall] %d ops in %.2fs  %.0f ops/s\n",
			total, r.total.Seconds(), float64(total)/r.total.Seconds())
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// counter is an atomic int64 without importing sync/atomic's verbosity twice.
type counter struct {
	mu sync.Mutex
	v  int64
}

func (c *counter) add(n int64) int64 { c.mu.Lock(); c.v += n; v := c.v; c.mu.Unlock(); return v }
func (c *counter) load() int64       { c.mu.Lock(); v := c.v; c.mu.Unlock(); return v }

// ---- Zipfian generator (Gray et al.), scrambled via userKey ----

type zipf struct {
	n            uint64
	theta        float64
	zetan, zeta2 float64
	alpha, eta   float64
}

func newZipf(n uint64, theta float64) *zipf {
	z := &zipf{n: n, theta: theta}
	z.zeta2 = zetaStatic(2, theta)
	z.zetan = zetaStatic(n, theta)
	z.alpha = 1.0 / (1.0 - theta)
	z.eta = (1 - math.Pow(2.0/float64(n), 1-theta)) / (1 - z.zeta2/z.zetan)
	return z
}

func zetaStatic(n uint64, theta float64) float64 {
	sum := 0.0
	for i := uint64(0); i < n; i++ {
		sum += 1.0 / math.Pow(float64(i+1), theta)
	}
	return sum
}

// zipfNext returns a Zipfian-distributed index in [0, n).
func (z *zipf) zipfNext(rng *rand.Rand) uint64 {
	u := rng.Float64()
	uz := u * z.zetan
	if uz < 1.0 {
		return 0
	}
	if uz < 1.0+math.Pow(0.5, z.theta) {
		return 1
	}
	return uint64(float64(z.n) * math.Pow(z.eta*u-z.eta+1, z.alpha))
}

// pick draws a key index for the given distribution over a keyspace of size hi.
func (z *zipf) pick(rng *rand.Rand, kind distKind, hi int64) uint64 {
	if hi <= 0 {
		return 0
	}
	switch kind {
	case distUniform:
		return uint64(rng.Int63n(hi))
	case distLatest:
		// Skew toward the most recently inserted keys.
		d := z.zipfNext(rng) % uint64(hi)
		return uint64(hi-1) - d
	default: // zipfian
		return z.zipfNext(rng) % uint64(hi)
	}
}

// ---- latency histogram ----

type hist struct {
	lats []time.Duration
}

func (h *hist) record(d time.Duration) { h.lats = append(h.lats, d) }
func (h *hist) count() int             { return len(h.lats) }
func (h *hist) merge(o *hist)          { h.lats = append(h.lats, o.lats...) }

func (h *hist) pct(p float64) time.Duration {
	if len(h.lats) == 0 {
		return 0
	}
	i := int(p * float64(len(h.lats)-1))
	return h.lats[i]
}

func (h *hist) report(name string) {
	sort.Slice(h.lats, func(i, j int) bool { return h.lats[i] < h.lats[j] })
	fmt.Printf("%-7s %9d ops   p50=%-9s p95=%-9s p99=%-9s p99.9=%-9s max=%s\n",
		name, len(h.lats), h.pct(0.50), h.pct(0.95), h.pct(0.99), h.pct(0.999), h.pct(1.0))
}
