// Command basalt is the client CLI: get, put, del, scan, and bench against
// a running basalt-server.
package main

import (
	"context"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"sort"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	basaltv1 "github.com/LovRanRan/basalt/api/basalt/v1"
)

// Exit codes: 0 ok, 1 error, 2 usage, 3 key not found.
const (
	exitOK = iota
	exitErr
	exitUsage
	exitNotFound
)

const usage = `usage: basalt [-addr host:port] [-timeout dur] <command> [args]

commands:
  get [-hex] <key>                        print the value; exit 3 when absent
  put <key> <value>
  del <key>
  scan [-start k] [-end k] [-limit n]     print key<TAB>value lines
  bench [-n ops] [-value-size b] [-read-ratio f] [-concurrency c]
`

func main() {
	os.Exit(cliMain(os.Args[1:], os.Stdout, os.Stderr))
}

func cliMain(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("basalt", flag.ContinueOnError)
	fs.SetOutput(stderr)
	addr := fs.String("addr", "127.0.0.1:7654", "server address")
	timeout := fs.Duration("timeout", 10*time.Second, "per-call timeout")
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}
	if fs.NArg() == 0 {
		fmt.Fprint(stderr, usage)
		return exitUsage
	}

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintln(stderr, "basalt:", err)
		return exitErr
	}
	defer func() { _ = conn.Close() }()
	c := basaltv1.NewKVServiceClient(conn)
	call := func() (context.Context, context.CancelFunc) {
		return context.WithTimeout(context.Background(), *timeout)
	}

	cmd, rest := fs.Arg(0), fs.Args()[1:]
	switch cmd {
	case "get":
		sub := flag.NewFlagSet("get", flag.ContinueOnError)
		sub.SetOutput(stderr)
		asHex := sub.Bool("hex", false, "print the value as hex")
		if err := sub.Parse(rest); err != nil || sub.NArg() != 1 {
			fmt.Fprintln(stderr, "usage: basalt get [-hex] <key>")
			return exitUsage
		}
		ctx, cancel := call()
		defer cancel()
		resp, err := c.Get(ctx, &basaltv1.GetRequest{Key: []byte(sub.Arg(0))})
		if err != nil {
			return rpcErr(stderr, err)
		}
		if !resp.GetFound() {
			fmt.Fprintln(stderr, "basalt: key not found")
			return exitNotFound
		}
		if *asHex {
			fmt.Fprintln(stdout, hex.EncodeToString(resp.GetValue()))
		} else {
			fmt.Fprintf(stdout, "%s\n", resp.GetValue())
		}
		return exitOK

	case "put":
		if len(rest) != 2 {
			fmt.Fprintln(stderr, "usage: basalt put <key> <value>")
			return exitUsage
		}
		ctx, cancel := call()
		defer cancel()
		if _, err := c.Put(ctx, &basaltv1.PutRequest{Key: []byte(rest[0]), Value: []byte(rest[1])}); err != nil {
			return rpcErr(stderr, err)
		}
		return exitOK

	case "del":
		if len(rest) != 1 {
			fmt.Fprintln(stderr, "usage: basalt del <key>")
			return exitUsage
		}
		ctx, cancel := call()
		defer cancel()
		if _, err := c.Delete(ctx, &basaltv1.DeleteRequest{Key: []byte(rest[0])}); err != nil {
			return rpcErr(stderr, err)
		}
		return exitOK

	case "scan":
		sub := flag.NewFlagSet("scan", flag.ContinueOnError)
		sub.SetOutput(stderr)
		start := sub.String("start", "", "inclusive start key")
		end := sub.String("end", "", "exclusive end key")
		limit := sub.Uint64("limit", 0, "max pairs (0 = unlimited)")
		if err := sub.Parse(rest); err != nil || sub.NArg() != 0 {
			fmt.Fprintln(stderr, "usage: basalt scan [-start k] [-end k] [-limit n]")
			return exitUsage
		}
		ctx, cancel := call()
		defer cancel()
		stream, err := c.Scan(ctx, &basaltv1.ScanRequest{Start: []byte(*start), End: []byte(*end), Limit: *limit})
		if err != nil {
			return rpcErr(stderr, err)
		}
		for {
			resp, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				return exitOK
			}
			if err != nil {
				return rpcErr(stderr, err)
			}
			for _, kv := range resp.GetPairs() {
				fmt.Fprintf(stdout, "%s\t%s\n", kv.GetKey(), kv.GetValue())
			}
		}

	case "bench":
		sub := flag.NewFlagSet("bench", flag.ContinueOnError)
		sub.SetOutput(stderr)
		n := sub.Int("n", 10_000, "total operations")
		valueSize := sub.Int("value-size", 100, "value bytes")
		readRatio := sub.Float64("read-ratio", 0.5, "fraction of reads")
		concurrency := sub.Int("concurrency", 4, "worker goroutines")
		if err := sub.Parse(rest); err != nil {
			return exitUsage
		}
		return bench(c, stdout, stderr, *n, *valueSize, *readRatio, *concurrency, *timeout)

	default:
		fmt.Fprintf(stderr, "basalt: unknown command %q\n%s", cmd, usage)
		return exitUsage
	}
}

func rpcErr(stderr io.Writer, err error) int {
	fmt.Fprintf(stderr, "basalt: rpc failed: %s: %s\n", status.Code(err), status.Convert(err).Message())
	if status.Code(err) == codes.InvalidArgument {
		return exitUsage
	}
	return exitErr
}

func bench(c basaltv1.KVServiceClient, stdout, stderr io.Writer, n, valueSize int, readRatio float64, concurrency int, timeout time.Duration) int {
	value := make([]byte, valueSize)
	for i := range value {
		value[i] = byte('a' + i%26)
	}
	key := func(i int) []byte { return fmt.Appendf(nil, "bench-%09d", i) }

	var mu sync.Mutex
	var lats []time.Duration
	var wg sync.WaitGroup
	errCh := make(chan error, concurrency)
	per := n / concurrency
	start := time.Now()
	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			local := make([]time.Duration, 0, per)
			for i := 0; i < per; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), timeout)
				t0 := time.Now()
				var err error
				if rng.Float64() < readRatio {
					_, err = c.Get(ctx, &basaltv1.GetRequest{Key: key(rng.Intn(n))})
				} else {
					_, err = c.Put(ctx, &basaltv1.PutRequest{Key: key(rng.Intn(n)), Value: value})
				}
				cancel()
				if err != nil {
					errCh <- err
					return
				}
				local = append(local, time.Since(t0))
			}
			mu.Lock()
			lats = append(lats, local...)
			mu.Unlock()
		}(int64(w))
	}
	wg.Wait()
	select {
	case err := <-errCh:
		fmt.Fprintln(stderr, "basalt: bench:", err)
		return exitErr
	default:
	}
	total := time.Since(start)
	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
	pct := func(p float64) time.Duration { return lats[int(p*float64(len(lats)-1))] }
	fmt.Fprintf(stdout, "bench: %d ops in %.2fs (%.0f ops/s) read-ratio=%.2f p50=%s p95=%s p99=%s\n",
		len(lats), total.Seconds(), float64(len(lats))/total.Seconds(), readRatio, pct(0.50), pct(0.95), pct(0.99))
	return exitOK
}
