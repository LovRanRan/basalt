package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	basalt "github.com/LovRanRan/basalt"
	basaltv1 "github.com/LovRanRan/basalt/api/basalt/v1"
)

func TestConfigPrecedence(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cfg.yaml")
	if err := os.WriteFile(path, []byte("listen: \"127.0.0.1:9999\"\ndata_dir: from-file\nlog_level: warn\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := loadConfig([]string{"-config", path, "-data-dir", "from-flag"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Listen != "127.0.0.1:9999" {
		t.Fatalf("file value lost: %q", cfg.Listen)
	}
	if cfg.DataDir != "from-flag" {
		t.Fatalf("flag must beat file: %q", cfg.DataDir)
	}
	if cfg.LogLevel != "warn" {
		t.Fatalf("file log level lost: %q", cfg.LogLevel)
	}
	if cfg.MetricsListen != defaultConfig().MetricsListen {
		t.Fatalf("default lost: %q", cfg.MetricsListen)
	}

	if err := os.WriteFile(path, []byte("no_such_field: 1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := loadConfig([]string{"-config", path}); err == nil {
		t.Fatal("unknown config field must fail fast")
	}
	if _, err := loadConfig([]string{"-log-level", "loud"}); err == nil {
		t.Fatal("bad log level must fail fast")
	}
}

func TestServeMetricsAndGracefulShutdown(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	cfg := defaultConfig()
	cfg.DataDir = dataDir
	cfg.Listen = "127.0.0.1:0"
	cfg.MetricsListen = "127.0.0.1:0"
	cfg.WALSync = false

	addrCh := make(chan [2]string, 1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	go func() {
		done <- run(ctx, cfg, logger, func(g, m net.Addr) {
			addrCh <- [2]string{g.String(), m.String()}
		})
	}()
	var addrs [2]string
	select {
	case addrs = <-addrCh:
	case err := <-done:
		t.Fatalf("server exited early: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("server never became ready")
	}

	conn, err := grpc.NewClient(addrs[0], grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	c := basaltv1.NewKVServiceClient(conn)
	for i := 0; i < 50; i++ {
		if _, err := c.Put(context.Background(), &basaltv1.PutRequest{
			Key:   []byte(fmt.Sprintf("key-%03d", i)),
			Value: []byte("v"),
		}); err != nil {
			t.Fatal(err)
		}
	}
	got, err := c.Get(context.Background(), &basaltv1.GetRequest{Key: []byte("key-007")})
	if err != nil || !got.GetFound() {
		t.Fatalf("get over the wire: %v %v", got, err)
	}

	resp, err := http.Get("http://" + addrs[1] + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"basalt_rpc_requests_total", "basalt_rpc_duration_seconds", "basalt_engine_flushes_total"} {
		if !strings.Contains(string(body), want) {
			t.Fatalf("/metrics missing %s", want)
		}
	}

	// Graceful shutdown: run returns nil and the data dir reopens cleanly
	// with the data intact.
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("shutdown: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("shutdown hung")
	}
	db, err := basalt.Open(dataDir, basalt.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if v, err := db.Get([]byte("key-007")); err != nil || string(v) != "v" {
		t.Fatalf("data dir not recoverable: %q %v", v, err)
	}
}
