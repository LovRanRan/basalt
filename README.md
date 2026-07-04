# Basalt

Distributed key-value store in Go — from-scratch LSM-tree storage engine (WAL, memtable, SSTables, leveled compaction) with Raft-replicated shards over gRPC.

> Basalt: molten writes cool into hard, ordered, layered stone.

## Status

**Phase 1 complete** — the single-node storage engine is done and crash-safe on every tested path:

- hand-written LSM tree: crc-framed WAL with torn-tail recovery, lock-free-read skiplist memtable, block-based SSTables (prefix compression, bloom filters, checksummed everything), background leveled compaction with tombstone GC
- crash-safe manifest: version-edit log, atomic `CURRENT` swaps, orphan collection — injected crash points at every rotation step recover consistently
- snapshot reads: refcounted read views pin their files across flushes and compactions; `Scan` never sees a torn batch or a post-snapshot write
- atomic `Batch`, hard-linked `Checkpoint` (a checkpoint is itself a complete database), process-exclusive locking

Next: gRPC server + CLI (Phase 2), hand-written Raft replication (Phase 3), multi-raft sharding (Phase 4). Commit-level roadmap, live status, and the work log live in [progress.md](progress.md) — the single source of truth for project progression.

## Usage

```go
db, err := basalt.Open("/tmp/demo", basalt.Options{})
if err != nil { ... }
defer db.Close()

_ = db.Put([]byte("user:1001"), []byte("alice"))
v, err := db.Get([]byte("user:1001")) // "alice"

var b basalt.Batch // commits atomically: all-or-nothing, even across a crash
b.Put([]byte("user:1002"), []byte("bob"))
b.Delete([]byte("user:1001"))
_ = db.Apply(&b)

it := db.Scan([]byte("user:"), []byte("user:~")) // snapshot iterator
for ; it.Valid(); it.Next() {
    fmt.Printf("%s = %s\n", it.Key(), it.Value())
}
_ = it.Error()
it.Close()

_ = db.Checkpoint("/tmp/demo-backup") // hard-linked, point-in-time, instantly openable
```

## Benchmarks

Reference run on an Apple M-series laptop (Go 1.26, `-sync=false`; a synced WAL is bounded by F_FULLFSYNC on macOS at roughly one flush per write):

```
$ go run ./cmd/basalt-bench
basalt-bench: n=200000 value=100B sync=false concurrency=4
fillseq             200000 ops in    0.64s     313814 ops/s  p50=1.6µs   p95=3.5µs   p99=5.4µs
fillrandom          200000 ops in    0.69s     290535 ops/s  p50=1.8µs   p95=3.5µs   p99=5.5µs
readrandom          200000 ops in    0.34s     595929 ops/s  p50=3.3µs   p95=26µs    p99=81µs
readwhilewriting    200000 ops in    8.80s      22725 ops/s  p50=38.5µs  p95=584µs   p99=2.85ms
scan                 5.97M keys/s over 10 full scans
engine: flushes=106 compactions=86 flushed=260.9MB compacted=618.3MB
```

Hot paths (`go test -bench . ./...`): skiplist insert 338ns, memtable get 248ns, sstable point get 1.03µs, sstable iterator next 28.5ns, 4-way merge next 13ns.

CI runs `basalt-bench -smoke` on every push so the harness itself cannot rot.
