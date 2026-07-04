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

# Smoke: put/get through ONE node. The sharded front door routes each key
# to the group owning its slot, forwarding to that group's leader, so any
# node serves any key.
ADDR=127.0.0.1:9101
echo "smoke: put/get through node 1 (the front door routes by shard)"
"$RUNDIR/basalt" -addr "$ADDR" put hello world
"$RUNDIR/basalt" -addr "$ADDR" put another-shard value2
val=$("$RUNDIR/basalt" -addr "$ADDR" get hello)
echo "get hello -> $val"
[ "$val" = "world" ] || { echo "SMOKE FAILED: value mismatch"; exit 1; }
echo "scan:"
"$RUNDIR/basalt" -addr "$ADDR" scan
echo "cluster is up (3 nodes, 3 groups). Ctrl-C to stop."
wait
