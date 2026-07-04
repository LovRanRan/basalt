package cluster

import (
	"context"
	"errors"
	"strconv"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	basaltv1 "github.com/LovRanRan/basalt/api/basalt/v1"
)

// Client is a cluster-aware KV client: it caches the current leader and,
// on a not-leader redirect, follows the hint (or round-robins) and retries.
type Client struct {
	addrs   map[uint64]string
	ids     []uint64
	conns   map[uint64]basaltv1.KVServiceClient
	pool    []*grpc.ClientConn
	leader  uint64 // last known good node id
	maxHops int
}

// NewClient dials every node (id -> host:port for the KV service).
func NewClient(addrs map[uint64]string) (*Client, error) {
	c := &Client{addrs: addrs, conns: map[uint64]basaltv1.KVServiceClient{}, maxHops: 10}
	for id, addr := range addrs {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			c.Close()
			return nil, err
		}
		c.pool = append(c.pool, conn)
		c.conns[id] = basaltv1.NewKVServiceClient(conn)
		c.ids = append(c.ids, id)
		if c.leader == 0 {
			c.leader = id
		}
	}
	return c, nil
}

func (c *Client) Close() {
	for _, conn := range c.pool {
		_ = conn.Close()
	}
}

// leaderHint parses "not-leader:<id>" out of a FailedPrecondition status.
func leaderHint(err error) (uint64, bool) {
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.FailedPrecondition {
		return 0, false
	}
	if s, found := strings.CutPrefix(st.Message(), "not-leader:"); found {
		if id, perr := strconv.ParseUint(s, 10, 64); perr == nil {
			return id, true
		}
	}
	return 0, true // not-leader but no usable hint
}

// call retries fn against the cached leader, following redirects, until it
// succeeds or the hop budget is spent.
func (c *Client) call(ctx context.Context, fn func(cl basaltv1.KVServiceClient) error) error {
	target := c.leader
	for hop := 0; hop < c.maxHops; hop++ {
		cl, ok := c.conns[target]
		if !ok {
			target = c.ids[hop%len(c.ids)]
			continue
		}
		err := fn(cl)
		if err == nil {
			c.leader = target
			return nil
		}
		hint, isRedirect := leaderHint(err)
		if !isRedirect {
			return err
		}
		if hint != 0 && c.conns[hint] != nil {
			target = hint
		} else {
			// No hint: try the next node.
			target = c.ids[(indexOf(c.ids, target)+1)%len(c.ids)]
		}
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	return errors.New("cluster: no leader found within hop budget")
}

func indexOf(ids []uint64, v uint64) int {
	for i, id := range ids {
		if id == v {
			return i
		}
	}
	return 0
}

func (c *Client) Put(ctx context.Context, key, value []byte) error {
	return c.call(ctx, func(cl basaltv1.KVServiceClient) error {
		_, err := cl.Put(ctx, &basaltv1.PutRequest{Key: key, Value: value})
		return err
	})
}

func (c *Client) Delete(ctx context.Context, key []byte) error {
	return c.call(ctx, func(cl basaltv1.KVServiceClient) error {
		_, err := cl.Delete(ctx, &basaltv1.DeleteRequest{Key: key})
		return err
	})
}

// Get returns (value, true, nil) when found, (nil, false, nil) when absent.
func (c *Client) Get(ctx context.Context, key []byte) ([]byte, bool, error) {
	var val []byte
	var found bool
	err := c.call(ctx, func(cl basaltv1.KVServiceClient) error {
		resp, err := cl.Get(ctx, &basaltv1.GetRequest{Key: key})
		if err != nil {
			return err
		}
		val, found = resp.GetValue(), resp.GetFound()
		return nil
	})
	return val, found, err
}
