# Basalt architecture

Basalt is a distributed, replicated, linearizable key-value store built from
the ground up in Go: a hand-written LSM storage engine, a hand-written Raft
implementation, and a hash-slot sharding layer that multiplexes many Raft
groups over one process. Nothing in the consensus or storage path is a
third-party library.

## The stack

```
                         ┌──────────────────────────────┐
                         │        cluster.Client        │  caches leader, follows
                         │  Put / Get / Delete / Scan   │  not-leader redirects,
                         └──────────────┬───────────────┘  backs off on a freeze
                                        │ gRPC
        any node ─────────────────────▼──────────────────────────── any node
        ┌───────────────────────────────────────────────────────────────────┐
        │  shardKV — the sharded front door                                   │
        │   • slot = crc32c(key) % 256 ; ShardMap[slot] -> group id           │
        │   • lead the owning group?  serve locally                           │
        │   • else forward to that group's leader (route-gid fenced)          │
        │   • Scan: scatter to every group, merge-sort, filter to the owner   │
        └───────────────┬───────────────────────────────────┬─────────────────┘
                        │                                   │
        ┌───────────────▼───────────┐        ┌──────────────▼────────────┐
        │  Node (group manager)      │        │  ShardService / RaftService │
        │  hosts groups {10,20,30…}  │◄──────►│  one gRPC conn per peer,    │
        │  one event loop per group  │  peer  │  every group multiplexed    │
        └───────────────┬───────────┘  traffic└─────────────────────────────┘
                        │
        ┌───────────────▼───────────────────────────────────────────────────┐
        │  raft group (one per shard-set)                                     │
        │   election + PreVote · log replication · persistence · snapshots    │
        │   ReadIndex linearizable reads · leadership transfer · membership   │
        └───────────────┬───────────────────────────────────────────────────┘
                        │ committed commands, in order
        ┌───────────────▼───────────────────────────────────────────────────┐
        │  StateMachine  →  LSM engine (one per group)                        │
        │   WAL · skiplist memtable · SSTables · leveled compaction · bloom   │
        │   manifest/versioning · atomic batch · hard-linked checkpoint       │
        └────────────────────────────────────────────────────────────────────┘
```

Every layer is independently testable and independently tested: the engine
against injected crashes, Raft against a seeded deterministic network
simulator, the cluster against real gRPC with kill / partition / slow-disk
fault injection and a seeded chaos runner.

## Storage engine (`.`, `internal/*`)

A log-structured merge tree.

- **WAL** (`internal/wal`) — crc-framed records with torn-tail recovery. A
  crash mid-write drops the partial record and everything before it survives.
- **Memtable** (`internal/memtable`) — a skiplist with lock-free reads;
  writes serialize on one lock, reads never block.
- **SSTables** (`internal/sstable`) — block-based, prefix-compressed,
  bloom-filtered, checksummed. Point gets skip blocks via the bloom filter and
  the index; iterators are a heap-merge across levels.
- **Compaction** — background leveled compaction with tombstone GC; L0 by file
  count, deeper levels by byte budget (10× per level).
- **Manifest** (`internal/manifest`) — a version-edit log with atomic
  `CURRENT` swaps and orphan collection; crash points injected at every
  rotation step recover consistently.
- **Snapshots & checkpoints** — refcounted read views pin their files across
  flushes and compactions, so `Scan` sees a stable point-in-time image;
  `Checkpoint` hard-links a complete, instantly-openable database.

Under Raft the engine runs with `DisableWAL` — the Raft log is the single
durability source, so double-logging every entry would be waste. The state
machine keeps its applied index and per-client dedup sessions in a reserved
key range (`\x00!raft/`) that rides in the same atomic batch as the user ops,
so recovery is an exact prefix replay, never an idempotent re-apply.

## Raft (`internal/raft`)

A hand-written implementation with a pure, deterministic core: `Step` /
`Tick` / `Ready` / `Advance`, no I/O and no clocks inside. This is what makes
it testable by a seeded simulator that runs thousands of randomized
network-reorder / crash schedules and asserts election safety, log matching,
and state-machine safety.

Implemented: leader election with **PreVote** (a partitioned node cannot
disrupt a stable leader by campaigning), log replication with the
consistency-check/backtrack protocol, durable `HardState` + log persistence,
log compaction + **InstallSnapshot** (streamed engine checkpoints), **ReadIndex**
linearizable reads with an optional leader lease, **leadership transfer**
(`MsgTimeoutNow`), and **single-server membership changes** (voters + learners,
one change in flight at a time).

## Cluster (`cluster`)

- **Multi-raft** — a `Node` hosts many independent groups behind one event
  loop each, multiplexing all of them over a single gRPC connection per peer
  (messages tagged by group id).
- **Sharding** (`internal/shard`) — 256 fixed hash slots; an immutable,
  epoch-versioned `ShardMap` assigns each slot to a group. Fixed slots (rather
  than a consistent-hash ring) make ownership explicit and migration a clean
  per-slot operation. The epoch is a fencing token distributed monotonically.
- **Front door** — any node serves any request by routing to the owning
  group's leader (forwarding when it isn't the leader), with a scatter-gather
  `Scan` that merge-sorts across groups and filters each key to its current
  owner.

## Write path

```
client.Put(k,v)
  → hits any node's shardKV
  → slot = crc32c(k)%256 ; gid = ShardMap[slot]
  → if slot is migrating: reject retryably (write freeze)
  → if this node leads gid: propose (re-checking the freeze on the loop) ─┐
  → else forward to gid's leader, stamped with the routed group id        │
                                                                          ▼
  raft group appends → replicates to a quorum → commits →
  StateMachine.Apply writes one atomic engine batch (user ops +
  applied index) → the proposal's waiter is signaled → Put returns
```

## Read path

Linearizable reads go through **ReadIndex**: the leader confirms it still
leads (a heartbeat round, or its lease), pins the current commit index, waits
until the engine has applied up to it, then reads. No entry is written, so
reads don't bloat the log.

## Rebalance: moving a slot

A slot handoff is single-owner-safe by *freezing*, not by the per-request
epoch fence alone:

```
1. FREEZE   publish a ShardMap version marking the slot "migrating" to every
            node → the front door rejects writes to that slot (retryable);
            reads keep flowing from the current owner.
2. BARRIER  a committed no-op on the source group → every write accepted
            before the freeze has now committed.
3. COPY     checkpoint the source engine, stream the slot's keys into the
            destination group's raft log (idempotent, resumable).
4. FLIP     publish a new map version: slot -> destination, freeze cleared.
5. CLEANUP  delete the migrated keys from the source.
```

If any step before the flip fails, the freeze is rolled back so a slot is
never stranded. The write freeze — held right up to the raft append via a
loop-serialized guard — is what guarantees the destination's copy is complete
before it takes ownership.

## Testing strategy

| Layer     | How it's tested |
|-----------|-----------------|
| Engine    | Injected crash points at every durability boundary; torn-WAL, torn-batch, checkpoint-during-churn |
| Raft      | Seeded deterministic simulator over thousands of randomized schedules; election/log/SM safety invariants |
| Cluster   | Real gRPC; fault injection (leader kill, network partition, slow disk); seeded chaos runner with read-after-ack + no-lost-writes history checks |
| Protocol  | `buf` lint + committed-stub drift check |
| Benchmarks| `basalt-bench` (engine db_bench) and `basalt-ycsb` (YCSB A–F); smoke runs in CI |
