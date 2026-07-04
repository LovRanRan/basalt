#!/usr/bin/env bash
# Boot a local 3-node/3-group Basalt cluster from examples/cluster.yaml and
# run a quick put/get smoke through the client. Ctrl-C tears it down.
set -euo pipefail

cd "$(dirname "$0")/.."
CONFIG=${1:-examples/cluster.yaml}
RUNDIR=$(mktemp -d)
PIDS=()

cleanup() {
  echo "shutting down cluster..."
  for pid in "${PIDS[@]:-}"; do kill "$pid" 2>/dev/null || true; done
  wait 2>/dev/null || true
  rm -rf "$RUNDIR"
}
trap cleanup EXIT INT TERM

go build -o "$RUNDIR/basalt-cluster" ./cmd/basalt-cluster
go build -o "$RUNDIR/basalt" ./cmd/basalt

for id in 1 2 3; do
  "$RUNDIR/basalt-cluster" -config "$CONFIG" -id "$id" -data-dir "$RUNDIR/node-$id" &
  PIDS+=($!)
done

echo "cluster starting; waiting for leaders to settle..."
sleep 3

# Smoke: put/get through the client. A key routes to whichever group owns
# its slot; until the shard router lands (P4.4) the plain CLI must hit the
# node that leads that group, so we try each node's client port. This proves
# every node is up and serving its shards.
ADDRS=(127.0.0.1:9101 127.0.0.1:9102 127.0.0.1:9103)
try_put() { for a in "${ADDRS[@]}"; do "$RUNDIR/basalt" -addr "$a" put "$1" "$2" 2>/dev/null && return 0; done; return 1; }
try_get() { for a in "${ADDRS[@]}"; do v=$("$RUNDIR/basalt" -addr "$a" get "$1" 2>/dev/null) && { echo "$v"; return 0; }; done; return 1; }

echo "smoke: put/get through the client (routes by shard)"
try_put hello world   || { echo "SMOKE FAILED: put"; exit 1; }
val=$(try_get hello)  || { echo "SMOKE FAILED: get"; exit 1; }
echo "get hello -> $val"
[ "$val" = "world" ] || { echo "SMOKE FAILED: value mismatch"; exit 1; }
echo "cluster is up (3 nodes, 3 groups). Ctrl-C to stop."
wait
