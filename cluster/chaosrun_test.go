//go:build chaos

package cluster

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"
)

// TestChaosRunner drives concurrent client traffic against a 3-node cluster
// while a seeded nemesis kills, reboots, and partitions nodes, recording an
// operation history that is checked afterward.
//
// The workload is write-once registers: every write targets a fresh unique
// key, and a write is retried (same key, same bytes — idempotent) until
// acknowledged. That choice is what makes the checker SOUND in the presence
// of indeterminate operations: a timed-out write may commit later, but since
// it can only ever install the one value its key will ever have, late commits
// cannot make history checks lie the way retried counter values can.
//
// Checked invariants:
//  1. Read visibility: a linearizable read that starts after a key's write
//     was acknowledged must find it, with exactly the written value — through
//     kills, reboots, and partitions.
//  2. No lost acknowledged writes: after the final heal + quiesce, every
//     acked key is present with its exact value on every replica's engine.
//
// The nemesis is seeded (BASALT_CHAOS_SEED to reproduce, logged on start) and
// keeps at most one fault active at a time so the cluster always has quorum.
func TestChaosRunner(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping chaos runner in -short")
	}
	seed := uint64(0xbada55)
	if env := os.Getenv("BASALT_CHAOS_SEED"); env != "" {
		if v, err := strconv.ParseUint(env, 10, 64); err == nil {
			seed = v
		}
	}
	dur := 6 * time.Second
	if env := os.Getenv("BASALT_CHAOS_SECONDS"); env != "" {
		if v, err := strconv.Atoi(env); err == nil && v > 0 {
			dur = time.Duration(v) * time.Second
		}
	}
	t.Logf("chaos runner: seed=%d duration=%v (set BASALT_CHAOS_SEED to reproduce)", seed, dur)

	ids := []uint64{1, 2, 3}
	k := startKillable(t, ids)
	defer k.stopAll()
	k.waitLeader(40 * time.Second)

	c, err := NewClient(k.kvAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// --- history ---
	type ack struct {
		key string
		val string
		at  time.Time // when the ack returned
	}
	var histMu sync.Mutex
	var acked []ack
	violation := make(chan string, 16)
	report := func(format string, args ...any) {
		select {
		case violation <- fmt.Sprintf(format, args...):
		default:
		}
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup

	// --- writers: unique write-once keys, retry until acked ---
	const writers = 3
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				key := fmt.Sprintf("w%d-%06d", w, i)
				val := fmt.Sprintf("v-%d-%d", w, i)
				for {
					ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
					err := c.Put(ctx, []byte(key), []byte(val))
					cancel()
					if err == nil {
						histMu.Lock()
						acked = append(acked, ack{key, val, time.Now()})
						histMu.Unlock()
						break
					}
					select {
					case <-stop:
						return
					case <-time.After(20 * time.Millisecond):
					}
				}
			}
		}(w)
	}

	// --- readers: pick an already-acked key; it must be found, exactly ---
	rrng := splitmix(seed ^ 0x5eed)
	const readers = 2
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			rng := splitmix(seed + uint64(r)*7919)
			for {
				select {
				case <-stop:
					return
				default:
				}
				histMu.Lock()
				n := len(acked)
				var target ack
				if n > 0 {
					target = acked[rng()%uint64(n)]
				}
				histMu.Unlock()
				if n == 0 {
					time.Sleep(10 * time.Millisecond)
					continue
				}
				start := time.Now()
				if !start.After(target.at) {
					continue // read must START after the ack for the check to bind
				}
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				v, found, err := c.Get(ctx, []byte(target.key))
				cancel()
				if err != nil {
					time.Sleep(10 * time.Millisecond) // fault window; retries exhausted — no claim
					continue
				}
				if !found {
					report("read of %s (acked %v before read start) returned not-found", target.key, start.Sub(target.at))
				} else if string(v) != target.val {
					report("read of %s = %q, want %q", target.key, v, target.val)
				}
			}
		}(r)
	}
	_ = rrng

	// --- nemesis: seeded, one fault at a time, quorum always preserved ---
	type faultState struct {
		kind string // "", "killed", "partitioned"
		node uint64
		heal func()
	}
	nrng := splitmix(seed)
	var fault faultState
	nemesisLog := []string{}
	deadline := time.Now().Add(dur)
	for time.Now().Before(deadline) {
		dwell := 200 + time.Duration(nrng()%300)*time.Millisecond/time.Millisecond
		time.Sleep(time.Duration(dwell) * time.Millisecond)
		if fault.kind != "" {
			// recover
			switch fault.kind {
			case "killed":
				k.boot(fault.node)
				nemesisLog = append(nemesisLog, fmt.Sprintf("reboot %d", fault.node))
			case "partitioned":
				fault.heal()
				nemesisLog = append(nemesisLog, fmt.Sprintf("heal %d", fault.node))
			}
			fault = faultState{}
			continue
		}
		victim := ids[nrng()%uint64(len(ids))]
		if nrng()%2 == 0 {
			k.kill(victim)
			fault = faultState{kind: "killed", node: victim}
			nemesisLog = append(nemesisLog, fmt.Sprintf("kill %d", victim))
		} else {
			heal := partition(k, victim)
			fault = faultState{kind: "partitioned", node: victim, heal: heal}
			nemesisLog = append(nemesisLog, fmt.Sprintf("partition %d", victim))
		}
	}
	// final recovery
	switch fault.kind {
	case "killed":
		k.boot(fault.node)
	case "partitioned":
		fault.heal()
	}
	t.Logf("nemesis schedule: %v", nemesisLog)

	// Let traffic run briefly against the healed cluster, then stop.
	time.Sleep(500 * time.Millisecond)
	close(stop)
	wg.Wait()

	select {
	case v := <-violation:
		t.Fatalf("linearizability violation (seed %d): %s", seed, v)
	default:
	}

	histMu.Lock()
	finalAcks := append([]ack(nil), acked...)
	histMu.Unlock()
	if len(finalAcks) < 20 {
		t.Fatalf("only %d writes acked across %v of chaos; workload made no real progress", len(finalAcks), dur)
	}
	t.Logf("%d writes acknowledged through the chaos schedule", len(finalAcks))

	// No lost acknowledged writes: every acked key on every replica, exactly.
	// (Engine reads with a per-node retry: replicas converge on heal.)
	k.waitLeader(40 * time.Second)
	for _, id := range ids {
		deadline := time.Now().Add(60 * time.Second)
		for i := 0; i < len(finalAcks); {
			a := finalAcks[i]
			k.mu.Lock()
			n := k.nodes[id]
			k.mu.Unlock()
			if db := n.DB(); db != nil {
				if v, err := db.Get([]byte(a.key)); err == nil && string(v) == a.val {
					i++
					continue
				}
			}
			if time.Now().After(deadline) {
				t.Fatalf("node %d: acked key %s lost (seed %d)", id, a.key, seed)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
}
