//go:build e2e

// Package e2e drives the real binaries over real TCP: build both, run the
// server as a child process, exercise it through the CLI, SIGKILL it, and
// verify WAL recovery through the network path.
package e2e

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func buildBinaries(t *testing.T) (serverBin, cliBin string) {
	t.Helper()
	dir := t.TempDir()
	serverBin = filepath.Join(dir, "basalt-server")
	cliBin = filepath.Join(dir, "basalt")
	for bin, pkg := range map[string]string{serverBin: "../cmd/basalt-server", cliBin: "../cmd/basalt"} {
		out, err := exec.Command("go", "build", "-o", bin, pkg).CombinedOutput()
		if err != nil {
			t.Fatalf("build %s: %v\n%s", pkg, err, out)
		}
	}
	return serverBin, cliBin
}

func freeAddr(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := lis.Addr().String()
	_ = lis.Close()
	return addr
}

type serverProc struct {
	cmd    *exec.Cmd
	stderr *bytes.Buffer
}

func startServer(t *testing.T, bin, dataDir, addr, maddr string) *serverProc {
	t.Helper()
	sp := &serverProc{stderr: &bytes.Buffer{}}
	sp.cmd = exec.Command(bin, "-data-dir", dataDir, "-listen", addr, "-metrics-listen", maddr)
	sp.cmd.Stderr = sp.stderr
	if err := sp.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	return sp
}

// cli runs the client binary and returns its exit code and stdout.
func cli(t *testing.T, bin, addr string, args ...string) (int, string) {
	t.Helper()
	var out, errb bytes.Buffer
	cmd := exec.Command(bin, append([]string{"-addr", addr, "-timeout", "2s"}, args...)...)
	cmd.Stdout, cmd.Stderr = &out, &errb
	err := cmd.Run()
	code := 0
	if err != nil {
		var ee *exec.ExitError
		if !errorsAs(err, &ee) {
			t.Fatalf("cli %v: %v (%s)", args, err, errb.String())
		}
		code = ee.ExitCode()
	}
	return code, out.String()
}

func errorsAs(err error, target **exec.ExitError) bool {
	ee, ok := err.(*exec.ExitError)
	if ok {
		*target = ee
	}
	return ok
}

// waitReady polls with the CLI until the server answers (found or
// not-found both mean "up").
func waitReady(t *testing.T, cliBin, addr string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		code, _ := cli(t, cliBin, addr, "get", "readiness-probe")
		if code == 0 || code == 3 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("server never became ready")
}

func TestEndToEnd(t *testing.T) {
	serverBin, cliBin := buildBinaries(t)
	dataDir := filepath.Join(t.TempDir(), "data")
	addr, maddr := freeAddr(t), freeAddr(t)

	srv := startServer(t, serverBin, dataDir, addr, maddr)
	defer func() { _ = srv.cmd.Process.Kill() }()
	waitReady(t, cliBin, addr)

	if code, _ := cli(t, cliBin, addr, "put", "greeting", "world"); code != 0 {
		t.Fatalf("put exit %d\nserver logs:\n%s", code, srv.stderr.String())
	}
	if code, out := cli(t, cliBin, addr, "get", "greeting"); code != 0 || out != "world\n" {
		t.Fatalf("get = %d %q", code, out)
	}
	for i := 0; i < 20; i++ {
		if code, _ := cli(t, cliBin, addr, "put", fmt.Sprintf("scan-%02d", i), "v"); code != 0 {
			t.Fatalf("put %d failed", i)
		}
	}
	if code, out := cli(t, cliBin, addr, "scan", "-start", "scan-", "-end", "scan-~", "-limit", "5"); code != 0 || bytes.Count([]byte(out), []byte("\n")) != 5 {
		t.Fatalf("scan = %d %q", code, out)
	}
	if code, _ := cli(t, cliBin, addr, "del", "greeting"); code != 0 {
		t.Fatal("del failed")
	}
	if code, _ := cli(t, cliBin, addr, "get", "greeting"); code != 3 {
		t.Fatalf("deleted key exit = %d, want 3", code)
	}
	if code, _ := cli(t, cliBin, addr, "put", "survivor", "of-the-crash"); code != 0 {
		t.Fatal("put survivor failed")
	}

	// Crash: SIGKILL, no shutdown of any kind.
	if err := srv.cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = srv.cmd.Wait()

	srv2 := startServer(t, serverBin, dataDir, addr, maddr)
	defer func() { _ = srv2.cmd.Process.Kill() }()
	waitReady(t, cliBin, addr)
	if code, out := cli(t, cliBin, addr, "get", "survivor"); code != 0 || out != "of-the-crash\n" {
		t.Fatalf("post-crash get = %d %q\nserver logs:\n%s", code, out, srv2.stderr.String())
	}
	if code, _ := cli(t, cliBin, addr, "get", "greeting"); code != 3 {
		t.Fatalf("tombstone lost in crash: exit %d", code)
	}

	// Graceful shutdown exits 0.
	if err := srv2.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- srv2.cmd.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("graceful shutdown exit: %v\nlogs:\n%s", err, srv2.stderr.String())
		}
	case <-time.After(15 * time.Second):
		t.Fatal("graceful shutdown hung")
	}
	if !bytes.Contains(srv2.stderr.Bytes(), []byte("shutting down")) {
		t.Fatalf("missing shutdown log:\n%s", srv2.stderr.String())
	}
	_ = os.RemoveAll(dataDir)
}
