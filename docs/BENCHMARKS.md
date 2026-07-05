# Benchmarks

Two drivers, both self-contained and both smoke-run in CI so the harness can't
rot:

- `cmd/basalt-bench` — engine-level `db_bench`-style workloads.
- `cmd/basalt-ycsb` — the standard YCSB workload mixes (A–F) with a Zipfian
  request distribution and per-operation latency histograms; drives either the
  in-process engine or a live cluster.

Numbers below are a reference run on an Apple M-series laptop (Go 1.26). They
are illustrative, not a leaderboard — the point is the *shape*: the engine is
microsecond-scale; replication adds the network + quorum + fsync cost you'd
expect, and the read/write ratio moves throughput exactly as it should.

## Engine — db_bench (`basalt-bench`)

100 B values, `-sync=false` (a synced WAL is bounded by `F_FULLFSYNC` on macOS
at roughly one flush per write):

```
fillseq             200000 ops    313814 ops/s   p50=1.6µs   p95=3.5µs   p99=5.4µs
fillrandom          200000 ops    290535 ops/s   p50=1.8µs   p95=3.5µs   p99=5.5µs
readrandom          200000 ops    595929 ops/s   p50=3.3µs   p95=26µs    p99=81µs
readwhilewriting    200000 ops     22725 ops/s   p50=38.5µs  p95=584µs   p99=2.85ms
scan                5.97M keys/s over 10 full scans
```

Hot paths (`go test -bench . ./...`): skiplist insert 338 ns, memtable get
248 ns, sstable point get 1.03 µs, sstable iterator next 28.5 ns, 4-way merge
next 13 ns.

## Engine — YCSB (`basalt-ycsb -engine`)

100 000 records, 100 000 ops, 8 threads, 100 B values, Zipfian θ=0.99:

| Workload | Mix | Throughput | Read p50 / p99 | Write p50 / p99 |
|----------|-----|-----------:|----------------|-----------------|
| A update-heavy | 50% read / 50% update | 319 k ops/s | 1.2 µs / 416 µs | 2.7 µs / 423 µs |
| B read-heavy   | 95% read / 5% update  | 452 k ops/s | 2.7 µs / 256 µs | 4.5 µs / 263 µs |
| C read-only    | 100% read             | 542 k ops/s | 4.1 µs / 168 µs | — |
| D read-latest  | 95% read / 5% insert  | 780 k ops/s | 0.8 µs / 197 µs | 3.8 µs / 204 µs (insert) |
| E scan-heavy   | 95% scan / 5% insert  | 129 k ops/s | scan p50 23.5 µs / p99 459 µs | 6.3 µs / 223 µs (insert) |
| F read-mod-write | 50% read / 50% RMW  | 264 k ops/s | 1.2 µs / 321 µs | RMW 4.8 µs / 505 µs |

The tail (p99.9 ≈ low-ms) is compaction: a background merge briefly contends
with foreground writes. Read-latest (D) is fastest because the hot set is the
freshly-written tail, still in the memtable.

## Cluster — YCSB (`basalt-ycsb -cluster`)

The same driver against a booted 3-node cluster (each write goes through Raft:
gRPC round-trip → append → quorum replication → apply). This is the honest
cost of linearizable replication, not a storage-engine microbenchmark:

| Workload | Throughput | Read p50 / p99 | Update p50 / p99 |
|----------|-----------:|----------------|------------------|
| A (50/50) | ~216 ops/s | 12 ms / 46 ms | 55 ms / 118 ms |
| B (95/5)  | ~997 ops/s | 2.8 ms / 47 ms | 24 ms / 58 ms |

Writes are an order of magnitude slower than reads (quorum + persistence per
write); read-heavy B is ~5× A's throughput because most ops avoid the write
path via ReadIndex. Batching, pipelining proposals, and relaxing the WAL sync
policy are the obvious levers and are left as future work.

## Reproducing

```sh
# engine
go run ./cmd/basalt-bench
go run ./cmd/basalt-ycsb -engine - -workload a -records 100000 -ops 100000

# cluster: boot 3 nodes, then point the driver at their client addresses
make cluster-up          # or: for id in 1 2 3; do basalt-cluster -config examples/cluster.yaml -id $id & done
go run ./cmd/basalt-ycsb -cluster 1=127.0.0.1:9101,2=127.0.0.1:9102,3=127.0.0.1:9103 -workload a
```
