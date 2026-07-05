package cluster

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	basalt "github.com/LovRanRan/basalt"
	basaltv1 "github.com/LovRanRan/basalt/api/basalt/v1"
	"github.com/LovRanRan/basalt/internal/raft"
)

// snapChunk is how many bytes one streamed snapshot chunk carries.
const snapChunk = 256 << 10

// snapKey identifies one in-flight snapshot transfer (group, destination).
type snapKey struct{ gid, to uint64 }

// sendSnapshot handles a group's MsgSnap: checkpoint the group's engine and
// stream it to the lagging follower, which installs it and rejoins from the
// boundary. One transfer per (group, follower) at a time; raft re-requests if
// a transfer is lost, so failures here are simply dropped.
func (n *Node) sendSnapshot(gid uint64, m raft.Message) {
	key := snapKey{gid, m.To}
	n.mu.Lock()
	if n.snapInFlight == nil {
		n.snapInFlight = map[snapKey]bool{}
	}
	if n.snapInFlight[key] {
		n.mu.Unlock()
		return
	}
	n.snapInFlight[key] = true
	peer := n.peers[m.To]
	n.mu.Unlock()
	if peer == nil {
		n.snapDone(key)
		return
	}

	go func() {
		defer n.snapDone(key)
		g := n.Group(gid)
		if g == nil {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		// Checkpoint on the group loop so applied/term are stable. Any
		// boundary at or past the requested one serves.
		staging := filepath.Join(n.groupDir(gid), fmt.Sprintf("snapsend-%d-%d", m.To, migSeq.Add(1)))
		var index, term uint64
		err := g.runAdmin(ctx, func() error {
			i, t, serr := g.sm.Snapshot(staging)
			index, term = i, t
			return serr
		})
		if err != nil {
			return
		}
		defer func() { _ = os.RemoveAll(staging) }()

		stream, err := peer.InstallSnapshot(ctx)
		if err != nil {
			return
		}
		if err := streamCheckpoint(stream, staging, gid, m.Term, index, term); err != nil {
			return
		}
		_, _ = stream.CloseAndRecv()
	}()
}

func (n *Node) snapDone(key snapKey) {
	n.mu.Lock()
	delete(n.snapInFlight, key)
	n.mu.Unlock()
}

// streamCheckpoint walks the checkpoint tree and ships every file in chunks.
func streamCheckpoint(stream basaltv1.RaftService_InstallSnapshotClient, root string, gid, leaderTerm, index, term uint64) error {
	return filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer func() { _ = f.Close() }()
		buf := make([]byte, snapChunk)
		sent := false
		for {
			m, rerr := f.Read(buf)
			if m > 0 || !sent {
				if serr := stream.Send(&basaltv1.InstallSnapshotRequest{
					Group: gid, Term: leaderTerm, SnapIndex: index, SnapTerm: term,
					File: rel, Data: buf[:m],
				}); serr != nil {
					return serr
				}
				sent = true
			}
			if errors.Is(rerr, io.EOF) {
				return nil
			}
			if rerr != nil {
				return rerr
			}
		}
	})
}

// InstallSnapshot receives a streamed checkpoint from a leader and installs
// it: the group goes briefly offline, its engine is replaced by the
// checkpoint, its raft storage is rewritten to the snapshot boundary, and it
// reopens applied exactly at the boundary — ready for the log tail.
func (rs *raftServer) InstallSnapshot(stream basaltv1.RaftService_InstallSnapshotServer) error {
	var staging string
	var gid, leaderTerm, index, term uint64
	var cur *os.File
	var curName string
	closeCur := func() error {
		if cur == nil {
			return nil
		}
		// The raft-log rewrite after install is durable, so the checkpoint
		// bytes must be too: a machine crash must never leave a log whose
		// boundary points at engine state that evaporated.
		err := cur.Sync()
		if cerr := cur.Close(); err == nil {
			err = cerr
		}
		cur = nil
		return err
	}
	defer func() {
		_ = closeCur()
		if staging != "" {
			_ = os.RemoveAll(staging)
		}
	}()

	for {
		chunk, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		if staging == "" {
			gid, leaderTerm = chunk.GetGroup(), chunk.GetTerm()
			index, term = chunk.GetSnapIndex(), chunk.GetSnapTerm()
			if gid == 0 || index == 0 {
				return fmt.Errorf("cluster: malformed snapshot header")
			}
			staging = filepath.Join(rs.n.groupDir(gid), fmt.Sprintf("snaprecv-%d", migSeq.Add(1)))
			if err := os.MkdirAll(staging, 0o755); err != nil {
				return err
			}
		}
		name := chunk.GetFile()
		if name == "" || filepath.IsAbs(name) || name != filepath.Clean(name) || strings.Contains(name, "..") {
			return fmt.Errorf("cluster: unsafe snapshot path %q", name)
		}
		if name != curName {
			if err := closeCur(); err != nil {
				return err
			}
			dst := filepath.Join(staging, name)
			if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
				return err
			}
			f, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o644)
			if err != nil {
				return err
			}
			cur, curName = f, name
		}
		if len(chunk.GetData()) > 0 {
			if _, err := cur.Write(chunk.GetData()); err != nil {
				return err
			}
		}
	}
	if err := closeCur(); err != nil {
		return err
	}
	if staging == "" {
		return fmt.Errorf("cluster: empty snapshot stream")
	}
	if err := syncTree(staging); err != nil {
		return err
	}
	// The received tree must be a checkpoint at exactly the advertised
	// boundary.
	gotIndex, gotTerm, err := basalt.ReadSnapshotMeta(staging)
	if err != nil || gotIndex != index || gotTerm != term {
		return fmt.Errorf("cluster: snapshot meta (%d,%d,%v), want (%d,%d)", gotIndex, gotTerm, err, index, term)
	}
	if err := rs.n.installSnapshot(gid, leaderTerm, index, term, staging); err != nil {
		return err
	}
	return stream.SendAndClose(&basaltv1.InstallSnapshotResponse{})
}

// installSnapshot swaps a received checkpoint in as a group's state: stop the
// group, replace its engine, rewrite its raft storage to the boundary
// (preserving term/vote durability rules), and reopen. Serialized with
// AddGroup/other installs by addMu. A snapshot at or below what the group has
// already applied is discarded.
func (n *Node) installSnapshot(gid, leaderTerm, index, term uint64, staging string) error {
	n.addMu.Lock()
	defer n.addMu.Unlock()

	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return fmt.Errorf("cluster: node is closed")
	}
	g := n.groups[gid]
	if g == nil {
		n.mu.Unlock()
		return fmt.Errorf("cluster: group %d not hosted here", gid)
	}
	delete(n.groups, gid) // offline while the state swaps
	n.mu.Unlock()

	reopen := func() (*group, error) {
		return openGroup(gid, n.cfg.ID, n.peerIDs, n.groupDir(gid),
			n.cfg.ElectionTick, n.cfg.HeartbeatTick, n.cfg.TickInterval, n.cfg.SnapshotEvery, n.cfg.fsyncDelay, n.sendTo, n.sendSnapshot)
	}
	reinstall := func(ng *group, err error) error {
		n.mu.Lock()
		defer n.mu.Unlock()
		if n.closed {
			if ng != nil {
				_ = ng.close()
			}
			return fmt.Errorf("cluster: node closed during snapshot install")
		}
		if ng != nil {
			n.groups[gid] = ng
		}
		return err
	}

	_ = g.close() // loop stopped: raft/sm state is now safe to read

	hs := g.raft.HardStateNow()
	// Discard a snapshot we must not apply, keeping the group's current (more
	// advanced) state: a stale-term leader must not overwrite state a newer
	// term built (raft's InstallSnapshot term rule), and a boundary at or
	// below our commit index would regress the commit index and wipe already-
	// committed log tail — losing acknowledged writes. The COMMIT index is the
	// correct gauge (it is >= applied), so a boundary at or under a committed-
	// but-not-yet-applied entry is still discarded.
	if leaderTerm < hs.Term || index <= hs.Commit {
		ng, err := reopen()
		return reinstall(ng, err)
	}

	newTerm, newVote := hs.Term, hs.Vote
	if leaderTerm > newTerm {
		newTerm, newVote = leaderTerm, 0
	}
	dbDir := filepath.Join(n.groupDir(gid), "db")
	if err := basalt.ReplaceWithCheckpoint(staging, dbDir); err != nil {
		return reinstall(nil, fmt.Errorf("cluster: install checkpoint: %w", err))
	}
	st, _, err := raft.OpenStorage(filepath.Join(n.groupDir(gid), "raft", "state"))
	if err != nil {
		return reinstall(nil, err)
	}
	if err := st.Rewrite(raft.HardState{Term: newTerm, Vote: newVote, Commit: index}, index, term, nil); err != nil {
		_ = st.Close()
		return reinstall(nil, err)
	}
	if err := st.Close(); err != nil {
		return reinstall(nil, err)
	}
	ng, err := reopen()
	return reinstall(ng, err)
}

// syncTree fsyncs every directory under root (files are synced as they are
// closed), so the received checkpoint survives a machine crash.
func syncTree(root string) error {
	return filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil || !d.IsDir() {
			return err
		}
		dir, err := os.Open(path)
		if err != nil {
			return err
		}
		serr := dir.Sync()
		if cerr := dir.Close(); serr == nil {
			serr = cerr
		}
		return serr
	})
}
