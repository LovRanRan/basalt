# Basalt

Distributed key-value store in Go — from-scratch LSM-tree storage engine (WAL, memtable, SSTables, leveled compaction) with Raft-replicated shards over gRPC.

> Basalt: molten writes cool into hard, ordered, layered stone.

## Status

Early design phase.

## Planned architecture

- **Storage engine (single node)** — hand-written LSM-tree: write-ahead log, in-memory memtable (skip list), immutable SSTables, leveled compaction, bloom filters
- **Replication** — Raft consensus: leader election, log replication, snapshots
- **Sharding** — consistent hashing across multiple Raft groups
- **API** — gRPC `Get` / `Put` / `Delete` / `Scan`, plus a small CLI client

## Roadmap

Four phases — single-node LSM engine, networked server, hand-written Raft replication, multi-raft sharding and hardening.

The commit-level roadmap, live status, and work log are tracked in [progress.md](progress.md) — the single source of truth for project progression.
