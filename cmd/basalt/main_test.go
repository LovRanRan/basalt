package main

import (
	"bytes"
	"net"
	"strings"
	"testing"

	"google.golang.org/grpc"

	basalt "github.com/LovRanRan/basalt"
	basaltv1 "github.com/LovRanRan/basalt/api/basalt/v1"
	"github.com/LovRanRan/basalt/server"
)

// startServer serves the real gRPC service on an ephemeral TCP port.
func startServer(t *testing.T) string {
	t.Helper()
	db, err := basalt.Open(t.TempDir(), basalt.Options{DisableWALSync: true})
	if err != nil {
		t.Fatal(err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	basaltv1.RegisterKVServiceServer(srv, server.New(db))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(func() {
		srv.Stop()
		_ = db.Close()
	})
	return lis.Addr().String()
}

func run(t *testing.T, addr string, args ...string) (int, string, string) {
	t.Helper()
	var out, errb bytes.Buffer
	code := cliMain(append([]string{"-addr", addr}, args...), &out, &errb)
	return code, out.String(), errb.String()
}

func TestCLIRoundtrip(t *testing.T) {
	addr := startServer(t)

	if code, _, e := run(t, addr, "put", "greeting", "hello"); code != exitOK {
		t.Fatalf("put exit %d: %s", code, e)
	}
	code, out, _ := run(t, addr, "get", "greeting")
	if code != exitOK || out != "hello\n" {
		t.Fatalf("get = %d %q", code, out)
	}
	code, out, _ = run(t, addr, "get", "-hex", "greeting")
	if code != exitOK || out != "68656c6c6f\n" {
		t.Fatalf("get -hex = %d %q", code, out)
	}
	if code, _, _ := run(t, addr, "del", "greeting"); code != exitOK {
		t.Fatalf("del exit %d", code)
	}
	if code, _, _ := run(t, addr, "get", "greeting"); code != exitNotFound {
		t.Fatalf("get after del = %d, want %d", code, exitNotFound)
	}
}

func TestCLIScanAndExitCodes(t *testing.T) {
	addr := startServer(t)
	for _, kv := range [][2]string{{"a1", "1"}, {"a2", "2"}, {"b1", "3"}} {
		if code, _, e := run(t, addr, "put", kv[0], kv[1]); code != exitOK {
			t.Fatalf("put: %d %s", code, e)
		}
	}
	code, out, _ := run(t, addr, "scan", "-start", "a", "-end", "b")
	if code != exitOK {
		t.Fatalf("scan exit %d", code)
	}
	if out != "a1\t1\na2\t2\n" {
		t.Fatalf("scan output %q", out)
	}
	code, out, _ = run(t, addr, "scan", "-limit", "1")
	if code != exitOK || strings.Count(out, "\n") != 1 {
		t.Fatalf("limited scan = %d %q", code, out)
	}

	if code, _, _ := run(t, addr, "nope"); code != exitUsage {
		t.Fatalf("unknown command exit %d", code)
	}
	if code, _, _ := run(t, addr, "put", "only-key"); code != exitUsage {
		t.Fatalf("bad arity exit %d", code)
	}
	// Empty key is the server's InvalidArgument — surfaced as usage.
	if code, _, _ := run(t, addr, "put", "", "v"); code != exitUsage {
		t.Fatalf("empty key exit %d", code)
	}
}

func TestCLIBench(t *testing.T) {
	addr := startServer(t)
	code, out, errb := run(t, addr, "bench", "-n", "400", "-concurrency", "4", "-value-size", "32")
	if code != exitOK {
		t.Fatalf("bench exit %d: %s", code, errb)
	}
	if !strings.Contains(out, "ops/s") || !strings.Contains(out, "p99=") {
		t.Fatalf("bench output missing stats: %q", out)
	}
}
