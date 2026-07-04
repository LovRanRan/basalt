// Command basalt-server serves the Basalt engine over gRPC with structured
// logging, Prometheus metrics, and graceful shutdown.
package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"gopkg.in/yaml.v3"

	basalt "github.com/LovRanRan/basalt"
	basaltv1 "github.com/LovRanRan/basalt/api/basalt/v1"
	"github.com/LovRanRan/basalt/server"
)

type config struct {
	Listen         string `yaml:"listen"`
	MetricsListen  string `yaml:"metrics_listen"`
	DataDir        string `yaml:"data_dir"`
	MemTableSizeMB int64  `yaml:"memtable_size_mb"`
	LogLevel       string `yaml:"log_level"`
	WALSync        bool   `yaml:"wal_sync"`
}

func defaultConfig() config {
	return config{
		Listen:         "127.0.0.1:7654",
		MetricsListen:  "127.0.0.1:7655",
		DataDir:        "data",
		MemTableSizeMB: 4,
		LogLevel:       "info",
		WALSync:        true,
	}
}

// loadConfig resolves precedence flags > file > defaults: flags are
// registered against the file-merged values, so only flags the user
// actually set override the file.
func loadConfig(args []string) (config, error) {
	fs := flag.NewFlagSet("basalt-server", flag.ContinueOnError)
	cfgPath := fs.String("config", "", "YAML config file")
	listen := fs.String("listen", "", "gRPC listen address")
	metricsListen := fs.String("metrics-listen", "", "metrics HTTP listen address")
	dataDir := fs.String("data-dir", "", "database directory")
	logLevel := fs.String("log-level", "", "debug|info|warn|error")
	if err := fs.Parse(args); err != nil {
		return config{}, err
	}

	cfg := defaultConfig()
	if *cfgPath != "" {
		buf, err := os.ReadFile(*cfgPath)
		if err != nil {
			return config{}, err
		}
		dec := yaml.NewDecoder(bytes.NewReader(buf))
		dec.KnownFields(true)
		if err := dec.Decode(&cfg); err != nil {
			return config{}, fmt.Errorf("config %s: %w", *cfgPath, err)
		}
	}
	if *listen != "" {
		cfg.Listen = *listen
	}
	if *metricsListen != "" {
		cfg.MetricsListen = *metricsListen
	}
	if *dataDir != "" {
		cfg.DataDir = *dataDir
	}
	if *logLevel != "" {
		cfg.LogLevel = *logLevel
	}
	switch cfg.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		return config{}, fmt.Errorf("config: unknown log_level %q", cfg.LogLevel)
	}
	return cfg, nil
}

func slogLevel(s string) slog.Level {
	switch s {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// metricsSet wires per-method RPC counters/latencies plus engine gauges.
type metricsSet struct {
	reg  *prometheus.Registry
	reqs *prometheus.CounterVec
	lat  *prometheus.HistogramVec
}

func newMetrics(db *basalt.DB) *metricsSet {
	m := &metricsSet{
		reg: prometheus.NewRegistry(),
		reqs: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "basalt_rpc_requests_total",
			Help: "RPCs by method and status code.",
		}, []string{"method", "code"}),
		lat: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "basalt_rpc_duration_seconds",
			Help:    "RPC latency by method.",
			Buckets: prometheus.ExponentialBuckets(1e-5, 4, 10),
		}, []string{"method"}),
	}
	m.reg.MustRegister(m.reqs, m.lat)
	engineGauge := func(name, help string, f func(basalt.Metrics) uint64) {
		m.reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: name, Help: help},
			func() float64 { return float64(f(db.Metrics())) }))
	}
	engineGauge("basalt_engine_flushes_total", "Memtable flushes.", func(s basalt.Metrics) uint64 { return s.Flushes })
	engineGauge("basalt_engine_compactions_total", "Compactions.", func(s basalt.Metrics) uint64 { return s.Compactions })
	engineGauge("basalt_engine_flush_bytes_total", "Bytes written by flushes.", func(s basalt.Metrics) uint64 { return s.FlushBytes })
	engineGauge("basalt_engine_compaction_bytes_total", "Bytes written by compactions.", func(s basalt.Metrics) uint64 { return s.CompactionBytes })
	return m
}

func (m *metricsSet) observe(method string, code string, d time.Duration) {
	m.reqs.WithLabelValues(method, code).Inc()
	m.lat.WithLabelValues(method).Observe(d.Seconds())
}

func unaryInterceptor(logger *slog.Logger, m *metricsSet) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		t0 := time.Now()
		resp, err := handler(ctx, req)
		d := time.Since(t0)
		code := status.Code(err).String()
		m.observe(info.FullMethod, code, d)
		logger.Info("rpc", "method", info.FullMethod, "code", code, "duration", d)
		return resp, err
	}
}

func streamInterceptor(logger *slog.Logger, m *metricsSet) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		t0 := time.Now()
		err := handler(srv, ss)
		d := time.Since(t0)
		code := status.Code(err).String()
		m.observe(info.FullMethod, code, d)
		logger.Info("rpc", "method", info.FullMethod, "code", code, "duration", d)
		return err
	}
}

// run serves until ctx is canceled, then drains in-flight RPCs and closes
// the engine so the WAL is synced and the data dir reopens cleanly. ready,
// when non-nil, receives the bound addresses (tests use ephemeral ports).
func run(ctx context.Context, cfg config, logger *slog.Logger, ready func(grpcAddr, metricsAddr net.Addr)) error {
	db, err := basalt.Open(cfg.DataDir, basalt.Options{
		MemTableSize:   cfg.MemTableSizeMB << 20,
		DisableWALSync: !cfg.WALSync,
	})
	if err != nil {
		return err
	}
	defer func() {
		if err := db.Close(); err != nil {
			logger.Error("close", "err", err)
		}
	}()

	m := newMetrics(db)
	grpcLis, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return err
	}
	metricsLis, err := net.Listen("tcp", cfg.MetricsListen)
	if err != nil {
		_ = grpcLis.Close()
		return err
	}

	srv := grpc.NewServer(
		grpc.UnaryInterceptor(unaryInterceptor(logger, m)),
		grpc.StreamInterceptor(streamInterceptor(logger, m)),
	)
	basaltv1.RegisterKVServiceServer(srv, server.New(db))

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{}))
	msrv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	if ready != nil {
		ready(grpcLis.Addr(), metricsLis.Addr())
	}
	logger.Info("serving", "grpc", grpcLis.Addr().String(), "metrics", metricsLis.Addr().String(), "data_dir", cfg.DataDir)

	errCh := make(chan error, 2)
	go func() { errCh <- srv.Serve(grpcLis) }()
	go func() {
		if err := msrv.Serve(metricsLis); !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	select {
	case <-ctx.Done():
		logger.Info("shutting down: draining in-flight rpcs")
		srv.GracefulStop()
		shCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = msrv.Shutdown(shCtx)
		return nil
	case err := <-errCh:
		srv.Stop()
		_ = msrv.Close()
		return err
	}
}

func main() {
	cfg, err := loadConfig(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, "basalt-server:", err)
		os.Exit(2)
	}
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slogLevel(cfg.LogLevel)}))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, cfg, logger, nil); err != nil {
		logger.Error("fatal", "err", err)
		os.Exit(1)
	}
}
