package cluster

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"sort"

	"gopkg.in/yaml.v3"

	"github.com/LovRanRan/basalt/internal/shard"
)

func bytesReader(b []byte) io.Reader { return bytes.NewReader(b) }

// FileConfig is the declarative cluster description loaded from YAML. Every
// node hosts every group (each group is a fully-replicated raft), and the
// slots are spread across the groups. Validation names the exact offending
// field.
//
//	groups: [10, 20, 30]
//	nodes:
//	  - id: 1
//	    raft: 127.0.0.1:8001
//	    client: 127.0.0.1:9001
//	  - { id: 2, raft: 127.0.0.1:8002, client: 127.0.0.1:9002 }
//	  - { id: 3, raft: 127.0.0.1:8003, client: 127.0.0.1:9003 }
type FileConfig struct {
	Groups []uint64     `yaml:"groups"`
	Nodes  []NodeConfig `yaml:"nodes"`
}

type NodeConfig struct {
	ID     uint64 `yaml:"id"`
	Raft   string `yaml:"raft"`
	Client string `yaml:"client"`
}

// LoadFileConfig reads and validates a cluster config file.
func LoadFileConfig(path string) (*FileConfig, error) {
	buf, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var fc FileConfig
	dec := yaml.NewDecoder(bytesReader(buf))
	dec.KnownFields(true)
	if err := dec.Decode(&fc); err != nil {
		return nil, fmt.Errorf("config %s: %w", path, err)
	}
	if err := fc.Validate(); err != nil {
		return nil, err
	}
	return &fc, nil
}

// Validate fails fast with a field-specific message.
func (fc *FileConfig) Validate() error {
	if len(fc.Groups) == 0 {
		return fmt.Errorf("config: groups must list at least one group id")
	}
	seenGroup := map[uint64]bool{}
	for i, g := range fc.Groups {
		if g == 0 {
			return fmt.Errorf("config: groups[%d] must be non-zero", i)
		}
		if seenGroup[g] {
			return fmt.Errorf("config: groups[%d]: duplicate group id %d", i, g)
		}
		seenGroup[g] = true
	}
	if len(fc.Nodes) == 0 {
		return fmt.Errorf("config: nodes must list at least one node")
	}
	if len(fc.Nodes)%2 == 0 {
		return fmt.Errorf("config: nodes has %d entries; an even cluster size cannot form a strict majority — use an odd count", len(fc.Nodes))
	}
	seenID := map[uint64]bool{}
	seenAddr := map[string]string{}
	for i, n := range fc.Nodes {
		if n.ID == 0 {
			return fmt.Errorf("config: nodes[%d].id must be non-zero", i)
		}
		if seenID[n.ID] {
			return fmt.Errorf("config: nodes[%d]: duplicate node id %d", i, n.ID)
		}
		seenID[n.ID] = true
		if n.Raft == "" {
			return fmt.Errorf("config: nodes[%d].raft address is required", i)
		}
		if n.Client == "" {
			return fmt.Errorf("config: nodes[%d].client address is required", i)
		}
		if prev, dup := seenAddr[n.Raft]; dup {
			return fmt.Errorf("config: nodes[%d].raft %q already used by %s", i, n.Raft, prev)
		}
		seenAddr[n.Raft] = fmt.Sprintf("nodes[%d].raft", i)
		if prev, dup := seenAddr[n.Client]; dup {
			return fmt.Errorf("config: nodes[%d].client %q already used by %s", i, n.Client, prev)
		}
		seenAddr[n.Client] = fmt.Sprintf("nodes[%d].client", i)
	}
	return nil
}

// Peers returns the id -> raft-address map every node shares.
func (fc *FileConfig) Peers() map[uint64]string {
	peers := map[uint64]string{}
	for _, n := range fc.Nodes {
		peers[n.ID] = n.Raft
	}
	return peers
}

// ClientAddrs returns the id -> client-address map for a Client.
func (fc *FileConfig) ClientAddrs() map[uint64]string {
	addrs := map[uint64]string{}
	for _, n := range fc.Nodes {
		addrs[n.ID] = n.Client
	}
	return addrs
}

// NodeByID returns the config for one node.
func (fc *FileConfig) NodeByID(id uint64) (NodeConfig, bool) {
	for _, n := range fc.Nodes {
		if n.ID == id {
			return n, true
		}
	}
	return NodeConfig{}, false
}

// SortedGroups returns the group ids in ascending order.
func (fc *FileConfig) SortedGroups() []uint64 {
	g := append([]uint64(nil), fc.Groups...)
	sort.Slice(g, func(i, j int) bool { return g[i] < g[j] })
	return g
}

// ShardMap builds the initial slot assignment spread across the groups.
func (fc *FileConfig) ShardMap() *shard.ShardMap {
	return shard.NewShardMap(fc.SortedGroups())
}
