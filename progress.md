# Basalt Progress

**This file is the single source of truth for project progression.**

Protocol — after every completed commit:
1. **Dashboard** — update current position, counts, blockers, date.
2. **Roadmap** — check the commit's box. Scope changes must be edited here *before* coding starts.
3. **Logs** — append an entry: date, commit id, what landed, key decisions / benchmark numbers.

No work happens outside the roadmap without amending it here first.

---

## Dashboard

| | |
|---|---|
| Current phase | Phase 1 — single-node LSM storage engine |
| Next commit | P1.7 — `feat(engine): open, put, delete, and get with wal replay and memtable flush to l0` |
| Commits done | 6 / 39 (P1: 6/11 · P2: 0/5 · P3: 0/11 · P4: 0/12) |
| Blockers | none |
| Last updated | 2026-07-04 |

---

## Roadmap

### Phase 1 — single-node LSM storage engine (6/11)

Goal: embeddable engine package with `Open / Get / Put / Delete / Scan / Close`, crash-safe on every path, with benchmarks.

- [x] **P1.1** `chore(build): bootstrap go module, ci pipeline, and internal key codec` — package layout (`internal/base|wal|memtable|sstable`, engine root); GitHub Actions running vet, golangci-lint, `-race` tests; internal key = user key + seqno + kind, comparator (user key asc, seqno desc), varint codecs.
  *Done when: CI green and the comparator property test holds on 10k random keys.*
- [x] **P1.2** `feat(memtable): skiplist memtable with ordered iteration and size accounting` — insert-only skiplist keyed by internal key, tombstones as Delete entries, SeekGE/Next iterator, single-writer/multi-reader via atomic pointers, approximate size tracking.
  *Done when: 100k-op differential test vs a reference sorted map passes race-clean.*
- [x] **P1.3** `feat(wal): segmented append-only log with crc32 records and torn-tail recovery` — fixed-size segments, length+crc32c framed write batches, configurable fsync policy, replay that truncates the torn tail, deletion of flushed segments.
  *Done when: torn-write injection at every tail byte offset recovers exactly the committed prefix.*
- [x] **P1.4** `feat(sstable): block-based writer with index block, bloom filter, and checksummed footer` — ~4KB data blocks with restart-point prefix compression, index block, ~10 bits/key bloom filter, versioned footer, crc32c per block, table properties for the manifest.
  *Done when: golden byte-layout test passes; out-of-order input fails with a typed error.*
- [x] **P1.5** `feat(sstable): reader with bloom-guarded point gets and seekable block iterators` — footer/checksum validation, bloom → index binary search → block restart-point search, lazy block loading, typed `ErrCorruption`.
  *Done when: randomized roundtrip on 100k-entry tables + every-byte corruption suite on a small table covering all block/filter/index/footer regions (every-byte-flip is O(size²) — infeasible on multi-MB tables) + hostile crafted-block suite.*
- [x] **P1.6** `feat(manifest): version-edit log and current pointer for crash-safe file tracking` — VersionEdit log on WAL framing, immutable Version snapshots, CURRENT swap via atomic rename+fsync, obsolete-file collector.
  *Done when: every injected crash point during manifest rotation recovers a consistent live-file set.*
- [ ] **P1.7** `feat(engine): open, put, delete, and get with wal replay and memtable flush to l0` — Open = manifest recovery + WAL replay; writes go WAL → memtable under a monotonic seqno; background flush to L0 with atomic manifest edit + WAL segment GC.
  *Done when: full crash matrix (after WAL append / after L0 write / after manifest edit) recovers acked writes exactly, race-clean.*
- [ ] **P1.8** *(P1.5 note: the merge loop must check each sstable child's `Error()` whenever it goes invalid — exhaustion and failure look identical via `Valid()`; memtable iterators cannot fail.)* `feat(engine): merging iterator, scan, version pinning, and seqno read snapshots` — k-way min-heap merge across memtables + SSTables, newest-seqno visibility, tombstone suppression, `Scan(start, end)`; **plus refcounted Version pinning and seqno-based read snapshots** so live iterators survive concurrent flush/compaction file retirement. *(Amended per design review: pinning moved here from the compaction commit — its own mid-scan-flush tests need it.)*
  *Done when: differential get/scan model test passes across forced multi-table states, including mid-scan flushes.*
- [ ] **P1.9** `feat(compaction): background leveled compaction with tombstone gc and atomic manifest commits` — level scoring (L0 by file count, L1+ by ~10x byte fanout), overlap-based input picking, shadowed-entry and bottom-level tombstone dropping, atomic file-swap manifest edit; reads proceed via the P1.8 pinned versions. *(P1.4 note: add `Writer.EstimatedSize()` — offset + pending data/index blocks — for output-file splitting; a counting io.Writer lags by the pending blocks.)*
  *Done when: 1M-op churn test holds level invariants + model equivalence, incl. crash-mid-compaction recovery.*
- [ ] **P1.10** `feat(engine): atomic write batch and consistent checkpoint api` — atomic multi-op `WriteBatch`; `Checkpoint()` = flush + hard-link live SSTables under a pinned Version + checkpoint manifest; `OpenFromCheckpoint` with atomic data-dir swap; stable snapshot iterator over a checkpoint. *(Inserted per design review: Raft apply, Raft snapshots, and shard rebalance all depend on these hooks. P1.2 constraint: batch ops need consecutive per-op seqnos — base..base+n-1, visibility published after the last add — because the memtable enforces internal-key uniqueness by panic.)*
  *Done when: checkpoint taken under concurrent writes reopens to a consistent point-in-time state; batch is all-or-nothing across crashes.*
- [ ] **P1.11** `bench(engine): workload harness reporting throughput and p50/p99 latency` — db_bench-style CLI (fillseq/fillrandom/readrandom/readwhilewriting/scan), HDR histograms, engine counters exposing write amplification; go benchmarks for hot paths; reference run in README.
  *Done when: all workloads complete in CI smoke mode emitting throughput and p50/p99.*

### Phase 2 — networked single-node server (0/5)

Goal: Basalt as a real service — gRPC API, CLI client, observability, Docker + CI. *(Design review cut a redundant standalone CI commit; its Makefile folds into P2.1.)*

- [ ] **P2.1** `feat(api): protobuf kv service definition with generated grpc stubs` — `api/basalt/v1/kv.proto` (unary Get/Put/Delete, streaming Scan), buf lint + breaking-change + codegen-drift checks in CI, committed generated code, Makefile.
  *Done when: buf lint + drift check pass in CI and stubs compile in `go build ./...`.*
- [ ] **P2.2** `feat(server): grpc service implementation wrapping the lsm engine` — engine errors → canonical gRPC codes; Scan streams fixed-size batches honoring limits and context cancellation.
  *Done when: full CRUD + streaming scan pass over bufconn with correct status codes, race-clean.*
- [ ] **P2.3** `feat(cmd): server binary with config, structured logging, and prometheus metrics` — YAML config with flag overrides, slog JSON logging via interceptors, `/metrics` with per-method histograms + engine gauges, graceful SIGTERM drain.
  *Done when: binary starts from config, serves RPCs, exposes populated /metrics, exits cleanly with a recoverable data dir.*
- [ ] **P2.4** `feat(cli): basalt client with get, put, del, scan, and bench subcommands` — gRPC client CLI, per-call timeouts, distinct exit codes, HDR-histogram bench subcommand.
  *Done when: all subcommands work against a live server; CLI tests green in CI.*
- [ ] **P2.5** `build(release): dockerfile and end-to-end smoke test wired into ci` — multi-stage distroless image; e2e test drives the real CLI against a real server including kill + restart WAL recovery.
  *Done when: CI builds the image and the e2e job passes, incl. data surviving server kill/restart.*

### Phase 3 — hand-written Raft replication (0/11)

Goal: Raft from the paper (no etcd/hashicorp), LSM engine as the replicated state machine, linearizable reads, survives leader kills.

- [ ] **P3.1** `feat(raft): pure node state machine with terms, roles, and in-memory log` — deterministic transport-free `Step(msg)/Tick()` core emitting a Ready struct (etcd pattern, hand-written); all I/O outside the core.
  *Done when: a node is drivable entirely by Step/Tick in unit tests with zero goroutines/timers/sockets.*
- [ ] **P3.2** `feat(raft): leader election with randomized timeouts, requestvote, and prevote` — Figure 2 transitions, [T, 2T) randomized timeouts from an injectable rand, vote-once-per-term with log up-to-dateness check; **PreVote so partitioned nodes don't depose healthy leaders on heal**. *(Amended per design review: Phase 4 partition suites need PreVote for non-flaky liveness.)*
  *Done when: 3-node in-memory cluster elects exactly one leader per term across 1000 seeded runs.*
- [ ] **P3.3** `test(raft): deterministic network simulator with seeded partitions and message loss` — virtual clock, in-memory network with drop/duplicate/reorder/delay/partition, single seeded RNG, failing seeds replay identically.
  *Done when: ElectionSafety verified over 2000+ seeds; any failing seed reproduces exactly.*
- [ ] **P3.4** `feat(raft): log replication with appendentries and commit index advancement` — nextIndex/matchIndex, consistency check, conflict-term fast backtracking, Figure 8 commit rule.
  *Done when: under 20% simulated loss + leader kills, every acked-committed entry is identical at the same index on all nodes.*
- [ ] **P3.5** `feat(raft): durable hard state and log persistence with crash-safe recovery` — currentTerm/votedFor/log reusing the P1.3 record *framing* (`AppendRecord`/`NextRecord`/`ScanRecords`) in its own segmented store: raft needs indexed entry lookup and truncate-suffix on conflicting entries, which the WAL Writer's append-only/torn-tail invariants deliberately do not support. Fsync-before-send ordering, torn-tail truncation on restart. *(Scope clarified by P1.3 review.)*
  *Done when: crash-restart at any fsync boundary rejoins with no double-vote, term regression, or committed-entry loss.*
- [ ] **P3.6** `feat(raft): apply committed entries to the lsm engine state machine` — StateMachine interface over the engine using P1.10 WriteBatch; clientID+seq dedup for exactly-once applies; completion futures. **Durability contract: engine WAL is disabled under raft — the raft log + snapshots are the sole durability source; recovery re-applies from the snapshot index via deterministic idempotent Apply that also rebuilds the dedup table.** *(Amended per design review; replica convergence asserted as logical scan equivalence, not byte-identical files.)*
  *Done when: after a 10k-op workload with two forced leader changes + a crash between apply and snapshot, all replicas scan-identical and every acked write present exactly once.*
- [ ] **P3.7** `feat(raft): log compaction and local snapshot create/restore` — size-triggered snapshot via P1.10 `Checkpoint()` (records lastIncludedIndex/term + client sessions), raft log prefix truncation, restart-from-snapshot. *(Split from the original snapshot commit per design review.)*
  *Done when: raft log stays bounded under sustained writes and a node restarts correctly from snapshot + log suffix.*
- [ ] **P3.8** `feat(raft): installsnapshot chunked transfer and receiver state swap` — leader detects follower behind first index, streams fixed-size chunks with offset/done semantics; receiver swaps state machine atomically via `OpenFromCheckpoint` and resets its log.
  *Done when: a follower behind the compacted log converges to scan-identical state via chunked InstallSnapshot.*
- [ ] **P3.9** `feat(raft): linearizable reads via readindex with optional leader lease` — ReadIndex quorum confirmation, follower forward/redirect, opt-in leader lease fast path with documented clock-drift tradeoff.
  *Done when: porcupine checker finds zero linearizability violations across seeded partition/failover histories.*
- [ ] **P3.10** `feat(server): grpc transport and client request routing with leader redirects` — real gRPC peer transport; NotLeader errors carry leader hints; client caches leader and retries idempotently (safe via P3.6 dedup).
  *Done when: a client given any node address completes reads/writes on a live 3-node cluster across a leader change.*
- [ ] **P3.11** `test(raft): leader-kill smoke test and replication benchmarks` — black-box process-level leader-kill smoke test plus replicated write throughput/latency benchmarks with baseline numbers recorded. *(Trimmed per design review — the full chaos harness lives in Phase 4.)*
  *Done when: smoke test survives repeated leader kills with zero acked-write loss; `make bench-raft` emits baselines.*

### Phase 4 — sharding and hardening (0/12)

Goal: multi-raft sharding, live rebalance, one real fault/chaos harness, benchmark report, docs. *(Design review: vnode ring replaced by fixed hash slots; cluster config lands before the router; shard-map distribution inserted; rebalance split in two.)*

- [ ] **P4.1** `refactor(raft): host multiple raft groups per process behind a group manager` — GroupManager supervising N groups with namespaced on-disk paths; transport multiplexed by group id; single-group becomes the degenerate case.
  *Done when: one process hosts multiple independently electing/replicating groups with the prior suite still green.*
- [ ] **P4.2** `feat(shard): fixed hash-slot table mapping keys to raft groups` — 256 hash slots → groupID (Redis-cluster style), deterministic cross-platform hash, versioned ShardMap type; pure routing math, no I/O. *(Replaced consistent-hash vnode ring per design review: slots make ownership contiguous and per-slot migration well-defined.)*
  *Done when: Lookup is deterministic and balanced; slot reassignment provably moves only the reassigned slots' keys (unit + fuzz tests).*
- [ ] **P4.3** `feat(cluster): static cluster config file and multi-node bootstrap tooling` — declarative YAML (nodes, groups, slot assignment, placement), fail-fast validation, `make cluster-up` boots a local 3-node/3-group cluster. *(Moved before the router per design review — the router's integration test needs this tooling.)*
  *Done when: `make cluster-up` starts a 3-node/3-group cluster from one config; malformed configs rejected with actionable errors.*
- [ ] **P4.4** `feat(router): shard-aware request routing with cross-group ordered scan` — front door routes point ops to owning group preserving leader redirects; Scan scatter-gathers all groups and merge-sorts into one ordered stream; client caches the shard map.
  *Done when: all four ops succeed against any node of the multi-group cluster with correct ownership and globally ordered scans.*
- [ ] **P4.5** `feat(raft): leadership transfer and single-server membership changes` — TimeoutNow-based leadership transfer (leader stops proposing, catches target up, transfers); single-server config changes applied on append; learners catch up before promotion; leader-only AddServer/RemoveServer RPCs. *(Leadership transfer added per design review — the remove-leader test depends on it.)*
  *Done when: a group grows/shrinks one server at a time under load, incl. removing the leader, with no lost writes or quorum stall.*
- [ ] **P4.6** `feat(cluster): versioned shard-map distribution with epoch fencing and dynamic group bootstrap` — push new shard-map versions to live nodes, epoch check rejects requests routed under a stale map, runtime instantiation of new raft groups on running processes. *(Inserted per design review — rebalance is unimplementable while the map is a startup-time file.)*
  *Done when: a new map version propagates to all nodes; stale-epoch requests are rejected retryably; a new group bootstraps at runtime.*
- [ ] **P4.7** `feat(rebalance): slot migration — checkpoint-copy a slot to its new owner` — per-slot migration using the P1.10 checkpoint iterator: stream a slot's keys from source group to target group as an idempotent, resumable admin operation.
  *Done when: a slot's data is byte-complete on the target while the source still serves it; re-running a crashed copy converges.*
- [ ] **P4.8** `feat(rebalance): slot migration — map flip, epoch fencing, and source cleanup` — flip the slot's ownership in a new map version, fence stale writers via P4.6 epochs, delete migrated keys from source; one slot at a time, idempotent coordinator. *(Fallback documented per design review: writes to a migrating slot may briefly reject with retryable errors instead of dual-routing.)*
  *Done when: adding a group and rebalancing loses/duplicates zero keys even when the coordinator crashes mid-migration.*
- [ ] **P4.9** `test(fault): fault-injection harness with leader-kill, partition, and slow-disk suites` — injectable transport faults (drop/delay/partition), fs wrapper (slow/failing fsync), process kill/restart helpers; deterministic suites incl. a partitioned stale leader that must not serve linearizable reads.
  *Done when: fault suites pass under -race with no acked write ever lost.*
- [ ] **P4.10** `test(chaos): seeded randomized chaos runner with history checking` — `cmd/chaos` drives concurrent clients while injecting P4.9 faults on a seeded schedule; porcupine-style history verification; failures dump seed + history + logs.
  *Done when: a seeded chaos run completes with the history checker reporting zero consistency violations.*
- [ ] **P4.11** `bench(ycsb): mixed-workload benchmark driver with latency histograms` — YCSB workloads A/B/C + scan-heavy, zipfian/uniform distributions, warm-up, HDR histograms, table + JSON output, 1-node vs 3-node sweep script.
  *Done when: `make bench` emits p50/p99 + throughput for all workloads against 1- and 3-node clusters.*
- [ ] **P4.12** `docs: benchmark report, architecture diagrams, and readme polish` — docs/benchmarks.md with methodology + honest bottleneck analysis; README architecture diagrams (LSM internals, raft group, multi-raft sharding, request path), quick-start, design-decisions-and-limitations section.
  *Done when: a newcomer goes from git clone to a running 3-node cluster and a reproduced benchmark using only the README.*

---

## Logs

*Newest first. Every entry: date · commit · what landed · decisions/numbers.*

- **2026-07-04** · **P1.6** `feat(manifest): version-edit log and current pointer for crash-safe file tracking` · Landed `internal/manifest`: tag-encoded VersionEdits over the WAL framing, immutable 7-level Version (L0 newest-first, L1+ disjoint-and-sorted — disjointness now *validated at the commit boundary* so a buggy P1.9 edit is refused, not logged), VersionSet with LevelDB-style rotate-on-every-open, atomic CURRENT swap, orphan-number dir scan, auto-collect at open, lock-free `Current()` via atomic.Pointer. Design bug caught while writing tests: crash mid-rotation orphans a manifest whose number the recovered NextFileNum would re-allocate → O_EXCL EEXIST wedges every reopen; fixed by scanning the dir past all on-disk numbers. Review highlights: **missing CURRENT with .sst present now refuses to open** (no crash produces that state; opening empty would let the collector destroy all data); **tail classification** — a damaged record with bytes after its declared end is provably not a torn tail → ErrCorrupt instead of silent truncation (which rotation+collection would make permanent); counter regressions refused; Apply stamps a copy (caller's edit never mutated) and rotation failure is decoupled from the already-durable edit; fd leak on rotation error paths fixed; rotation threshold anchored to snapshot size (avoids O(version) rewrite per edit once the snapshot alone exceeds RotateSize). Tests race-clean: crash injection at all 5 rotation steps + double-crash (recovery rotation crashing at every step, thrice-reopened), three torn-tail shapes vs provable mid-file damage, 200-edit randomized collector-never-deletes-live, orphan bump, level-move edit, codec corruption.

- **2026-07-04** · **P1.5** `feat(sstable): reader with bloom-guarded point gets and seekable block iterators` · Disk format closes the loop: `Reader` (eager footer/filter/index validation, lazy crc-verified data blocks, `Get(userKey, seq)` via bloom → index binary search → restart binary search), `Iterator` (First/SeekGE/Next/Error, lazy block hops), typed `ErrCorruption` everywhere. Review theme: **crc protects against accidents, not adversaries** — two blockers were crc-valid crafted files that panic the reader (uvarint sum wrapping the bounds check; sub-trailer restart keys hitting `base.Compare`), plus a quadratic-allocation DoS via prefix-extended index entries and 32-bit int truncation in restart parsing. All fixed centrally (overflow-safe two-part bounds check, trailer check moved into `decodeNext`, index requires full keys, u64 arithmetic) and pinned by a new hostile crafted-block suite — the every-byte-flip test can't reach post-crc validators, which is exactly how the panics shipped. Index/block disagreement now surfaces as corruption instead of a silent wrong answer. Tests race-clean: 100k roundtrip (iteration + 20k exact-seq gets + absent keys + 10k SeekGE vs reference), every-byte flips, hostile blocks/index, concurrent readers under -race, aliasing contracts. Done_when wording amended (small table for the O(size²) flip suite).

- **2026-07-03** · **P1.4** `feat(sstable): block-based writer with index block, bloom filter, and checksummed footer` · Landed `internal/sstable` (writer half): prefix-compressed data blocks with restart points, whole-table bloom (LevelDB hash + double hashing, now format-pinned), index block at restart interval 1 (pure binary search) mapping each block's **last key** → uvarint handle, 48-byte fixed footer (u64 handles, version, crc over first 36 bytes, magic `0xba5a17557ab1e000`), crc32c trailer per block; Properties (count/smallest/largest/size) returned for the manifest; sticky-error writer rejects out-of-order/duplicate/short keys (`ErrOutOfOrder`). Review (format + reader-fit lenses at xhigh) confirmed every P1.5 seek path works against this format unchanged, and caught: **u32 truncation in bloom** (read side could panic on hostile filter → now reads as "maybe"; write side rejects ≥2³² filter bits), **restart-offset u32 overflow = silent corruption with a valid crc** (BlockSize capped 1 GiB + overflow poisons the writer), and **two golden holes** — no multi-restart block and no version-straddling block cut were pinned; golden regenerated (RestartInterval 2 + a third `user:0002` version) with structural assertions so future `-update` runs can't silently lose those shapes. Empty-block encoding (restart [0], count 1) documented as a P1.5 trap; `EstimatedSize()` noted for P1.9. Tests race-clean incl. I/O-failure stickiness, caller-buffer scribbling, exact-boundary `>=` cut; bloom fpr 10 bits/key < 2.5% observed.

- **2026-07-03** · **P1.3 fixup** `fix(wal): check or explicitly discard file-close errors` · errcheck (CI lint) flagged unchecked `Close()` returns; syncDir/repairTornTail now propagate close errors, intentional discards made explicit. golangci-lint installed locally (brew) and added to the pre-commit loop so lint failures surface before push, not in CI.

- **2026-07-03** · **P1.3** `feat(wal): segmented append-only log with crc32 records and torn-tail recovery` · Landed `internal/wal`: record framing crc32c(len+payload)+len+payload (exported — manifest reuses it), segmented Writer (rotation fsyncs a segment before creating its successor), Replay, Batch encode/decode. Key invariant: **existence of segment N+1 proves N is durable and whole** — established at rotation *and* at open-time repair; hence a bad record in the newest segment is a silently-skipped torn tail, while damage in older segments (or a gap in segment ids) is loud `ErrCorrupt`. Self-caught bug pre-review: torn tails must be *physically truncated* at OpenWriter, or a second restart misreads them as corruption. Review (crash-consistency lens at xhigh) then found the real blocker: **writer had no sticky error state** — after a failed Append, a later successful one lands behind the partial frame and replay silently drops it; writer now poisons permanently on first I/O error (also kills fsyncgate retries). Also per review: repair fsyncs even clean segments (page cache ≠ durable), parent-dir dirent fsync, per-removal dir syncs + contiguity check, `Append` returns the record's segment id (rotation happens post-write; `SegmentID()` alone would mis-record wal-log-numbers in P1.7), `MaxRecordSize` (64 MiB) keeps whole-segment replay reads legitimate, DecodeBatch allocation capped, macOS F_FULLFSYNC documented. Exhaustive test now covers truncation + bit-flip at **every byte of the whole segment** (~600 cases), not just the tail; P3.5 roadmap scope corrected (raft reuses framing, not the Writer). Tests race-clean.

- **2026-07-03** · **P1.2** `feat(memtable): skiplist memtable with ordered iteration and size accounting` · Landed `internal/memtable`: insert-only skiplist over encoded internal keys (maxHeight 12, branch 1/4), single-writer/multi-reader via `atomic.Pointer` links published bottom-up after node init; `Get(userKey, seq)` via `AppendSeekKey`; live (non-snapshot) SeekGE/Next iterator; values copied on Add, reads capacity-capped against append-through; approximate size accounting drives future flush. Adversarial review (3 lenses, memory-model lens at xhigh) upgraded the tests, which is where the value was: **concurrent test originally couldn't catch publication bugs** — ascending inserts only ever splice at the skiplist tail, so readers never traverse a half-published node; now inserts follow a random permutation and scans SeekGE random windows. Differential test now targets version boundaries deterministically (query at e.seq and e.seq−1) instead of relying on random collisions to exercise the max-kind seek contract. Visibility guarantee re-stated in happens-before terms (timing alone promises nothing); P1.10 note added — batches need consecutive per-op seqnos, memtable panics on duplicate internal keys. Tests race-clean: 100k-op differential vs reference model, 30k random-order inserts against 4 concurrent readers, monotonic size, boundary/panic contracts.

- **2026-07-03** · **P1.1 fixup** `fix(ci): pin golangci-lint v2.12` · First real CI run on GitHub: test job green, lint job failed — golangci-lint v2.1.6 is built with Go 1.24 and refuses a `go 1.26` module ("Go language version used to build golangci-lint is lower than the targeted Go version"). Pinned v2.12 (May 2026, Go 1.26-compatible). Repo published: https://github.com/LovRanRan/basalt (public).

- **2026-07-03** · **P1.1** `chore(build): bootstrap go module, ci pipeline, and internal key codec` · Landed `internal/base`: internal key codec (56-bit seq + 8-bit kind packed little-endian in an 8-byte trailer, LevelDB-style), comparator (user key asc, then trailer desc), `AppendSeekKey`, varint length-prefixed framing with aliasing-capped decodes; CI (build / vet / gofmt / race tests + golangci-lint). Toolchain: Go 1.26.4 installed via Homebrew, go directive set to 1.26. Adversarial review (3 lenses, 12 findings) drove the notable decisions: **seek keys must carry the max kind** or SeekGE skips same-seq Puts at snapshot boundaries — helper added now so P1.2/P1.5/P1.8 can't each hardcode it wrong; kind validated at encode time (symmetric with seq, fails at the bug site); Compare's trust contract documented — disk-parsed keys must be length-validated before Compare, which constrains P1.4/P1.5's block format; CI fixes: setup-go cache off until a go.sum exists (hard-errors otherwise), golangci-lint-action v6+`latest` would install an unsupported v2 binary → pinned action v8 + v2.1, push triggers restricted to main to avoid double PR runs. Tests race-clean: 10k-key comparator property test, torn/overlong varint corruption cases, alias-cap tests, explicit prefix/empty-key/kind-tiebreak cases. Note: `internal/wal|memtable|sstable` dirs land with their own commits — git tracks no empty dirs.

- **2026-07-03** · *(pre-code)* · Project bootstrapped: name **Basalt** chosen via collision-vetted naming pass (runners-up: Kvasir, Quoral); repo scaffolded (git init on `main`, `go.mod` as `github.com/LovRanRan/basalt`, README with tagline + phase roadmap). Commit-level roadmap designed by four parallel design passes + one adversarial review; review amendments adopted: engine checkpoint/WriteBatch API inserted (P1.10), version pinning moved into the iterator commit (P1.8), raft-vs-engine-WAL durability contract defined (P3.6), snapshot commit split (P3.7/P3.8), PreVote added (P3.2), leadership transfer added (P4.5), vnode ring replaced with fixed hash slots (P4.2), shard-map distribution/epoch fencing inserted (P4.6), rebalance split (P4.7/P4.8), redundant Phase-2 CI commit cut, chaos harnesses consolidated into Phase 4. **Blocker: Go toolchain not installed.** No code yet — next: P1.1.
