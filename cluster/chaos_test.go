//go:build chaos

package cluster

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// killable is a cluster whose nodes can be killed and restarted on the same
// addresses, so a rotating leader can be repeatedly SIGKILL-equivalent
// stopped while the majority keeps a quorum.
type killable struct {
	t        testing.TB
	ids      []uint64
	peers    map[uint64]string
	kvAddr   map[uint64]string
	dataDirs map[uint64]string
	nodes    map[uint64]*Node
	srv      map[uint64]*Servers
	mu       sync.Mutex
}

func startKillable(t testing.TB, ids []uint64) *killable {
	t.Helper()
	k := &killable{
		t: t, ids: ids,
		peers: map[uint64]string{}, kvAddr: map[uint64]string{},
		dataDirs: map[uint64]string{}, nodes: map[uint64]*Node{}, srv: map[uint64]*Servers{},
	}
	for _, id := range ids {
		// Bind, read the address, keep it for rebinding after a kill.
		rl, ra := freePort(t)
		kl, ka := freePort(t)
		_ = rl.Close()
		_ = kl.Close()
		k.peers[id], k.kvAddr[id] = ra, ka
		k.dataDirs[id] = filepath.Join(t.TempDir(), fmt.Sprintf("n%d", id))
	}
	for _, id := range ids {
		k.boot(id)
	}
	return k
}

func (k *killable) boot(id uint64) {
	n, err := Open(Config{
		ID: id, Peers: k.peers, DataDir: k.dataDirs[id],
		ElectionTick: 10, HeartbeatTick: 1, TickInterval: 10 * time.Millisecond,
		SnapshotEvery: 500,
	})
	if err != nil {
		k.t.Fatalf("boot %d: %v", id, err)
	}
	rl, err := net.Listen("tcp", k.peers[id])
	if err != nil {
		k.t.Fatalf("rebind raft %d: %v", id, err)
	}
	kl, err := net.Listen("tcp", k.kvAddr[id])
	if err != nil {
		k.t.Fatalf("rebind kv %d: %v", id, err)
	}
	k.mu.Lock()
	k.nodes[id] = n
	k.srv[id] = n.Serve(rl, kl)
	k.mu.Unlock()
}

// kill stops a node hard and frees its addresses for a later restart.
func (k *killable) kill(id uint64) {
	k.mu.Lock()
	s, n := k.srv[id], k.nodes[id]
	delete(k.srv, id)
	delete(k.nodes, id)
	k.mu.Unlock()
	s.Raft.Stop()
	s.KV.Stop()
	_ = n.Close()
}

func (k *killable) leader() (uint64, bool) {
	k.mu.Lock()
	defer k.mu.Unlock()
	for id, n := range k.nodes {
		if _, _, isLeader := n.Status(); isLeader {
			return id, true
		}
	}
	return 0, false
}

func (k *killable) waitLeader(d time.Duration) uint64 {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if id, ok := k.leader(); ok {
			return id
		}
		time.Sleep(10 * time.Millisecond)
	}
	k.t.Fatal("no leader")
	return 0
}

func (k *killable) stopAll() {
	k.mu.Lock()
	defer k.mu.Unlock()
	for id, s := range k.srv {
		s.Raft.Stop()
		s.KV.Stop()
		_ = k.nodes[id].Close()
	}
}

func TestLeaderKillSmoke(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping chaos smoke test in -short")
	}
	ids := []uint64{1, 2, 3}
	k := startKillable(t, ids)
	defer k.stopAll()
	k.waitLeader(5 * time.Second)

	c, err := NewClient(k.kvAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// A writer stamps monotonically increasing values; we track the
	// highest value the cluster acknowledged so we can assert none is lost.
	var acked atomic.Int64
	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 0
		for {
			select {
			case <-stop:
				return
			default:
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			err := c.Put(ctx, []byte("counter"), []byte(fmt.Sprintf("%d", i)))
			cancel()
			if err == nil {
				acked.Store(int64(i))
				i++
			} else {
				time.Sleep(20 * time.Millisecond)
			}
		}
	}()

	// Kill the leader, let the majority recover, restart the dead node,
	// repeat. Each cycle the write loop must keep making progress.
	kills := 6
	for cycle := 0; cycle < kills; cycle++ {
		time.Sleep(300 * time.Millisecond)
		before := acked.Load()
		lead := k.waitLeader(5 * time.Second)
		k.kill(lead)
		// Majority elects a new leader and the writer resumes.
		k.waitLeader(8 * time.Second)
		deadline := time.Now().Add(8 * time.Second)
		for acked.Load() <= before+2 && time.Now().Before(deadline) {
			time.Sleep(20 * time.Millisecond)
		}
		if acked.Load() <= before+2 {
			close(stop)
			wg.Wait()
			t.Fatalf("cycle %d: no write progress after killing leader %d (acked %d -> %d)", cycle, lead, before, acked.Load())
		}
		k.boot(lead) // heal, restoring the quorum for the next cycle
	}

	close(stop)
	wg.Wait()
	final := acked.Load()
	if final < int64(kills) {
		t.Fatalf("only %d writes acked across %d kills", final, kills)
	}

	// Every acknowledged write survived: a linearizable read sees a
	// counter value at least as high as the last ack.
	k.waitLeader(5 * time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	v, found, err := c.Get(ctx, []byte("counter"))
	if err != nil || !found {
		t.Fatalf("final read: found=%v err=%v", found, err)
	}
	var got int64
	if _, serr := fmt.Sscanf(string(v), "%d", &got); serr != nil {
		t.Fatalf("counter value %q: %v", v, serr)
	}
	if got < final {
		t.Fatalf("acked write lost: counter=%d but acked=%d", got, final)
	}
	t.Logf("survived %d leader kills, %d writes acked, final counter %d", kills, final, got)
}

func BenchmarkReplicatedPut(b *testing.B) {
	ids := []uint64{1, 2, 3}
	k := startKillable(b, ids)
	defer k.stopAll()
	k.waitLeader(5 * time.Second)
	c, err := NewClient(k.kvAddr)
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()
	val := make([]byte, 100)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := c.Put(ctx, []byte(fmt.Sprintf("k%08d", i)), val); err != nil {
			b.Fatal(err)
		}
	}
}
