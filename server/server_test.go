package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	basalt "github.com/LovRanRan/basalt"
	basaltv1 "github.com/LovRanRan/basalt/api/basalt/v1"
)

// startServer runs the service over bufconn and returns a real client.
func startServer(t *testing.T) (*basalt.DB, basaltv1.KVServiceClient) {
	t.Helper()
	db, err := basalt.Open(t.TempDir(), basalt.Options{MemTableSize: 16 << 10, DisableWALSync: true})
	if err != nil {
		t.Fatal(err)
	}
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	basaltv1.RegisterKVServiceServer(srv, New(db))
	go func() { _ = srv.Serve(lis) }()

	conn, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		srv.Stop()
		_ = db.Close()
	})
	return db, basaltv1.NewKVServiceClient(conn)
}

func TestCRUDOverGRPC(t *testing.T) {
	_, c := startServer(t)
	ctx := context.Background()

	if _, err := c.Put(ctx, &basaltv1.PutRequest{Key: []byte("k1"), Value: []byte("v1")}); err != nil {
		t.Fatal(err)
	}
	got, err := c.Get(ctx, &basaltv1.GetRequest{Key: []byte("k1")})
	if err != nil || !got.GetFound() || string(got.GetValue()) != "v1" {
		t.Fatalf("get = %v, %v", got, err)
	}
	if _, err := c.Delete(ctx, &basaltv1.DeleteRequest{Key: []byte("k1")}); err != nil {
		t.Fatal(err)
	}
	got, err = c.Get(ctx, &basaltv1.GetRequest{Key: []byte("k1")})
	if err != nil || got.GetFound() {
		t.Fatalf("deleted key: found=%v err=%v", got.GetFound(), err)
	}
	// Absence is not an error.
	got, err = c.Get(ctx, &basaltv1.GetRequest{Key: []byte("never")})
	if err != nil || got.GetFound() {
		t.Fatalf("absent key: found=%v err=%v", got.GetFound(), err)
	}
}

func TestStatusCodes(t *testing.T) {
	db, c := startServer(t)
	ctx := context.Background()

	for _, call := range []func() error{
		func() error { _, err := c.Get(ctx, &basaltv1.GetRequest{}); return err },
		func() error { _, err := c.Put(ctx, &basaltv1.PutRequest{Value: []byte("v")}); return err },
		func() error { _, err := c.Delete(ctx, &basaltv1.DeleteRequest{}); return err },
	} {
		if got := status.Code(call()); got != codes.InvalidArgument {
			t.Fatalf("empty key: code = %v, want InvalidArgument", got)
		}
	}

	huge := make([]byte, 65<<20)
	if _, err := c.Put(ctx, &basaltv1.PutRequest{Key: []byte("k"), Value: huge},
		grpc.MaxCallSendMsgSize(70<<20)); status.Code(err) != codes.ResourceExhausted {
		// The default server receive limit (4 MiB) rejects it first —
		// also acceptable; assert it is a client-visible error either way.
		if err == nil {
			t.Fatal("oversized put must fail")
		}
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Get(ctx, &basaltv1.GetRequest{Key: []byte("k")}); status.Code(err) != codes.Unavailable {
		t.Fatalf("closed db: code = %v, want Unavailable", status.Code(err))
	}
}

func TestScanStreaming(t *testing.T) {
	_, c := startServer(t)
	ctx := context.Background()
	const n = 700 // forces multiple 256-pair batches
	for i := 0; i < n; i++ {
		if _, err := c.Put(ctx, &basaltv1.PutRequest{
			Key:   []byte(fmt.Sprintf("key-%05d", i)),
			Value: []byte(fmt.Sprintf("val-%05d", i)),
		}); err != nil {
			t.Fatal(err)
		}
	}

	collect := func(req *basaltv1.ScanRequest) []*basaltv1.KeyValue {
		t.Helper()
		stream, err := c.Scan(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		var pairs []*basaltv1.KeyValue
		msgs := 0
		for {
			resp, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			msgs++
			pairs = append(pairs, resp.GetPairs()...)
		}
		if len(pairs) > scanBatchPairs && msgs < 2 {
			t.Fatalf("expected multiple stream messages, got %d", msgs)
		}
		return pairs
	}

	all := collect(&basaltv1.ScanRequest{})
	if len(all) != n {
		t.Fatalf("full scan = %d pairs, want %d", len(all), n)
	}
	for i, kv := range all {
		if string(kv.GetKey()) != fmt.Sprintf("key-%05d", i) {
			t.Fatalf("pair %d out of order: %q", i, kv.GetKey())
		}
	}

	bounded := collect(&basaltv1.ScanRequest{Start: []byte("key-00100"), End: []byte("key-00200")})
	if len(bounded) != 100 || string(bounded[0].GetKey()) != "key-00100" {
		t.Fatalf("bounded scan = %d pairs starting %q", len(bounded), bounded[0].GetKey())
	}

	limited := collect(&basaltv1.ScanRequest{Limit: 42})
	if len(limited) != 42 {
		t.Fatalf("limited scan = %d pairs, want 42", len(limited))
	}
}

func TestScanCancellationReleasesIterator(t *testing.T) {
	db, c := startServer(t)
	ctx := context.Background()
	for i := 0; i < 2000; i++ {
		if _, err := c.Put(ctx, &basaltv1.PutRequest{
			Key:   []byte(fmt.Sprintf("key-%05d", i)),
			Value: make([]byte, 512),
		}); err != nil {
			t.Fatal(err)
		}
	}
	cctx, cancel := context.WithCancel(ctx)
	stream, err := c.Scan(cctx, &basaltv1.ScanRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatal(err)
	}
	cancel()
	for {
		if _, err := stream.Recv(); err != nil {
			break
		}
	}
	// The server-side iterator must have released its pin: Close drains
	// and would hang (or the readState refcount would leak) otherwise.
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}
