package main

import (
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	basalt "github.com/LovRanRan/basalt"
)

func TestWorkloadSpecs(t *testing.T) {
	for _, w := range []string{"a", "b", "c", "d", "e", "f", "A", "F"} {
		c := config{workload: w}
		if err := c.spec(); err != nil {
			t.Fatalf("workload %q: %v", w, err)
		}
		sum := c.read + c.update + c.insert + c.scan + c.rmw
		if sum < 0.999 || sum > 1.001 {
			t.Fatalf("workload %q proportions sum to %v, want 1.0", w, sum)
		}
	}
	if err := (&config{workload: "z"}).spec(); err == nil {
		t.Fatal("unknown workload must error")
	}
}

func TestRunAgainstEngine(t *testing.T) {
	db, err := basalt.Open(filepath.Join(t.TempDir(), "db"), basalt.Options{DisableWALSync: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	for _, w := range []string{"a", "c", "d", "e", "f"} {
		cfg := config{workload: w, records: 500, ops: 500, threads: 4, valueSize: 32, theta: 0.99, seed: 7}
		if err := cfg.spec(); err != nil {
			t.Fatal(err)
		}
		res, err := run(cfg, &engineStore{db: db})
		if err != nil {
			t.Fatalf("workload %q run: %v", w, err)
		}
		total := 0
		for _, h := range res.hists {
			total += h.count()
		}
		if total != cfg.ops {
			t.Fatalf("workload %q recorded %d ops, want %d", w, total, cfg.ops)
		}
	}
}

// TestZipfianIsSkewed checks the generator concentrates draws on a small hot
// set — the whole point of a Zipfian request distribution.
func TestZipfianIsSkewed(t *testing.T) {
	const n = 10000
	z := newZipf(n, 0.99)
	rng := rand.New(rand.NewSource(42))
	counts := make([]int, n)
	const draws = 200000
	for i := 0; i < draws; i++ {
		counts[z.pick(rng, distZipfian, n)]++
	}
	// The hottest 1% of keys should absorb a large majority of draws.
	hot := 0
	for i := 0; i < n/100; i++ {
		hot += counts[i]
	}
	if frac := float64(hot) / draws; frac < 0.5 {
		t.Fatalf("hottest 1%% of keys got only %.1f%% of draws; distribution not skewed", frac*100)
	}
}

func TestParseCluster(t *testing.T) {
	m, err := parseCluster("1=localhost:7001, 2=localhost:7002 ,3=h:9")
	if err != nil {
		t.Fatal(err)
	}
	if len(m) != 3 || m[1] != "localhost:7001" || m[3] != "h:9" {
		t.Fatalf("parsed %v", m)
	}
	if _, err := parseCluster("nope"); err == nil {
		t.Fatal("malformed spec must error")
	}
	if _, err := parseCluster(""); err == nil {
		t.Fatal("empty spec must error")
	}
}

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}
