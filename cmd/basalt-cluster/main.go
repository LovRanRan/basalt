// Command basalt-cluster starts one node of a sharded, replicated Basalt
// cluster from a declarative config file: it hosts every group in the
// config, serves the raft peer transport and a shard-routing KV service.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/LovRanRan/basalt/cluster"
)

func main() {
	cfgPath := flag.String("config", "", "cluster config YAML")
	id := flag.Uint64("id", 0, "this node's id (must appear in the config)")
	dataDir := flag.String("data-dir", "", "data directory (default: data/node-<id>)")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	if *cfgPath == "" || *id == 0 {
		fmt.Fprintln(os.Stderr, "usage: basalt-cluster -config <file> -id <node-id> [-data-dir <dir>]")
		os.Exit(2)
	}
	fc, err := cluster.LoadFileConfig(*cfgPath)
	if err != nil {
		logger.Error("config", "err", err)
		os.Exit(2)
	}
	self, ok := fc.NodeByID(*id)
	if !ok {
		logger.Error("config", "err", fmt.Sprintf("node id %d not in config", *id))
		os.Exit(2)
	}
	dir := *dataDir
	if dir == "" {
		dir = filepath.Join("data", fmt.Sprintf("node-%d", *id))
	}

	n, err := cluster.Open(cluster.Config{
		ID: *id, Peers: fc.Peers(), DataDir: dir,
		ElectionTick: 10, HeartbeatTick: 1, TickInterval: 50 * time.Millisecond,
		SnapshotEvery: 1000, Groups: fc.SortedGroups(),
	})
	if err != nil {
		logger.Error("open", "err", err)
		os.Exit(1)
	}
	raftLis, err := net.Listen("tcp", self.Raft)
	if err != nil {
		logger.Error("listen raft", "err", err)
		os.Exit(1)
	}
	kvLis, err := net.Listen("tcp", self.Client)
	if err != nil {
		logger.Error("listen client", "err", err)
		os.Exit(1)
	}
	srv := n.ServeSharded(raftLis, kvLis, fc.ShardMap())
	logger.Info("serving", "id", *id, "raft", self.Raft, "client", self.Client,
		"groups", fc.SortedGroups(), "data_dir", dir)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()
	logger.Info("shutting down")
	srv.Raft.GracefulStop()
	srv.KV.GracefulStop()
	if err := n.Close(); err != nil {
		logger.Error("close", "err", err)
		os.Exit(1)
	}
}
