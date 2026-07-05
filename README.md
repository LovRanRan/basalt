# Basalt

A distributed, replicated, linearizable key-value store in Go — a from-scratch
LSM storage engine, a from-scratch Raft implementation, and hash-slot sharding
with live rebalancing, all over gRPC. No third-party storage or consensus
library; the interesting parts are hand-written and heavily tested.

> Basalt: molten writes cool into hard, ordered, layered stone.

## What it is

- **Storage engine** — a hand-written LSM tree: crc-framed WAL with torn-tail
  recovery, lock-free-read skiplist memtable, block-based SSTables (prefix
  compression, bloom filters, checksums), background leveled compaction, a
  crash-safe version-edit manifest, snapshot reads, atomic batches, and
  hard-linked checkpoints.
- **Consensus** — a hand-written Raft with a pure deterministic core:
  election + PreVote, log replication, persistence, log compaction and
  streamed snapshot install, ReadIndex linearizable reads, leadership
  transfer, and single-server membership changes.
- **Distribution** — multi-raft (many groups per process over one connection
  per peer), 256 hash slots mapped to groups by an epoch-versioned shard map,
  a front door that routes/forwards any request to the owning leader, and
  scatter-gather scans.
- **Rebalancing** — online per-slot migration: freeze → barrier → copy → flip
  → cleanup, single-owner-safe across the handoff.
- **Hardening** — fault injection (leader kill, network partition, slow disk)
  and a seeded chaos runner that checks read-after-ack visibility and
  no-lost-writes through the chaos.

The commit-level roadmap, live status, and full work log are in
[progress.md](progress.md). Design details are in
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md); benchmark numbers and how to
reproduce them are in [docs/BENCHMARKS.md](docs/BENCHMARKS.md).

## Architecture at a glance

```
 cluster.Client ── gRPC ──►  shardKV front door  ──►  raft group  ──►  LSM engine
 (leader cache,             (slot = crc32c(key)     (election, log,   (WAL, memtable,
  redirects, freeze          %256 → ShardMap →       snapshots,        SSTables,
  back-off)                  owning group leader)     ReadIndex)        compaction)
```

Any node serves any key: it routes by the shard map to the owning group's
leader, forwarding when it isn't the leader itself. See
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the write/read/rebalance
flows and the testing strategy.

## Using the engine

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

## Running a cluster

```sh
make cluster-up   # boots a local 3-node / 3-group cluster from examples/cluster.yaml

# talk to it with the CLI
go run ./cmd/basalt -addr 127.0.0.1:9101 put user:1 alice
go run ./cmd/basalt -addr 127.0.0.1:9101 get user:1

# or benchmark it
go run ./cmd/basalt-ycsb -cluster 1=127.0.0.1:9101,2=127.0.0.1:9102,3=127.0.0.1:9103 -workload a
```

Commands: `basalt` (CLI), `basalt-server` (single-node gRPC server with a YAML
config, structured logging, Prometheus metrics), `basalt-cluster` (a cluster
member), `basalt-bench` (engine db_bench), `basalt-ycsb` (YCSB A–F).

## Benchmarks

Reference run on an Apple M-series laptop (full tables in
[docs/BENCHMARKS.md](docs/BENCHMARKS.md)):

- **Engine**: fillrandom 290 k ops/s, readrandom 596 k ops/s; YCSB read-only
  542 k ops/s, update-heavy 319 k ops/s (100 B values, `-sync=false`).
- **Cluster** (3 nodes, linearizable, Raft per write): YCSB read-heavy ~997
  ops/s, update-heavy ~216 ops/s — the honest cost of quorum replication and
  per-write persistence.

## Development

```sh
make build   # go build ./...
make test    # go test -race ./...
make lint    # golangci-lint
make vet
```

CI runs test / lint / proto-drift / e2e / chaos / docker on every push,
including `basalt-bench -smoke`, `basalt-ycsb` smoke, and a chaos-tagged suite
(leader kill, fault injection, the seeded chaos runner).
