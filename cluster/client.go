package cluster

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	basaltv1 "github.com/LovRanRan/basalt/api/basalt/v1"
)

// Freeze-retry pacing: a slot mid-handoff rejects writes on EVERY node, so
// the client backs off in place rather than rotating targets. A handoff is
// typically sub-second; the cap bounds a stuck one to ~10s before surfacing
// the retryable error to the caller.
const (
	freezeBackoff   = 25 * time.Millisecond
	maxFreezeWaits  = 400
	migratingPrefix = "slot-migrating:"
)

// isMigrating reports the cluster-wide write-freeze rejection.
func isMigrating(err error) bool {
	st, ok := status.FromError(err)
	return ok && st.Code() == codes.FailedPrecondition && strings.HasPrefix(st.Message(), migratingPrefix)
}

// Client is a cluster-aware KV client: it caches the current leader and,
// on a not-leader redirect, follows the hint (or round-robins) and retries.
// Safe for concurrent use.
type Client struct {
	addrs   map[uint64]string
	ids     []uint64
	conns   map[uint64]basaltv1.KVServiceClient
	pool    []*grpc.ClientConn
	leader  atomic.Uint64 // last known good node id
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
		if c.leader.Load() == 0 {
			c.leader.Store(id)
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
// succeeds or the hop budget is spent. A slot-migrating rejection backs off
// in place (every node rejects identically during a handoff, so rotating is
// useless) without spending the hop budget.
func (c *Client) call(ctx context.Context, fn func(cl basaltv1.KVServiceClient) error) error {
	target := c.leader.Load()
	freezeWaits := 0
	for hop := 0; hop < c.maxHops; hop++ {
		cl, ok := c.conns[target]
		if !ok {
			target = c.ids[hop%len(c.ids)]
			continue
		}
		err := fn(cl)
		if err == nil {
			c.leader.Store(target)
			return nil
		}
		if isMigrating(err) {
			if freezeWaits++; freezeWaits > maxFreezeWaits {
				return err // still retryable; the caller can back off longer
			}
			hop--
			select {
			case <-time.After(freezeBackoff):
			case <-ctx.Done():
				return ctx.Err()
			}
			continue
		}
		if hint, isRedirect := leaderHint(err); isRedirect {
			// Explicit not-leader: follow the hint, else round-robin.
			if hint != 0 && c.conns[hint] != nil {
				target = hint
			} else {
				target = c.ids[(indexOf(c.ids, target)+1)%len(c.ids)]
			}
		} else if isUnavailable(err) {
			// The node is down or unreachable: try the next one.
			target = c.ids[(indexOf(c.ids, target)+1)%len(c.ids)]
		} else {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	return errors.New("cluster: no leader found within hop budget")
}

// isUnavailable reports a transport-level failure (node down, connection
// refused, deadline) that another node might not have.
func isUnavailable(err error) bool {
	st, ok := status.FromError(err)
	if !ok {
		return false
	}
	switch st.Code() {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Aborted:
		return true
	default:
		return false
	}
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
