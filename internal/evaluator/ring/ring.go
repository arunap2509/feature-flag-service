package ring

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"

	"feature-flag-service/internal/constants"
)

// Ring implements an in-memory Consistent Hash Ring with virtual nodes
type Ring struct {
	mu           sync.RWMutex
	virtualNodes int
	hashRing     []uint32
	nodeMap      map[uint32]string
	members      map[string]struct{}
}

// New creates a new Consistent Hash Ring with a specified number of virtual nodes
func New(virtualNodes int) *Ring {
	if virtualNodes <= 0 {
		virtualNodes = constants.DefaultVirtualNodes
	}
	return &Ring{
		virtualNodes: virtualNodes,
		hashRing:     make([]uint32, 0),
		nodeMap:      make(map[uint32]string),
		members:      make(map[string]struct{}),
	}
}

func hashKey(key string) uint32 {
	h := sha256.Sum256([]byte(key))
	return binary.BigEndian.Uint32(h[:4])
}

// SetMembers atomically updates the ring to match the current active node membership
func (r *Ring) SetMembers(nodeIDs []string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.members = make(map[string]struct{})
	r.nodeMap = make(map[uint32]string)
	r.hashRing = make([]uint32, 0, len(nodeIDs)*r.virtualNodes)

	for _, nodeID := range nodeIDs {
		r.members[nodeID] = struct{}{}
		for i := 0; i < r.virtualNodes; i++ {
			vKey := fmt.Sprintf("%s#%d", nodeID, i)
			h := hashKey(vKey)
			r.hashRing = append(r.hashRing, h)
			r.nodeMap[h] = nodeID
		}
	}

	sort.Slice(r.hashRing, func(i, j int) bool {
		return r.hashRing[i] < r.hashRing[j]
	})
}

// GetOwner returns the physical node ID that owns the given key
func (r *Ring) GetOwner(key string) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.hashRing) == 0 {
		return "", false
	}

	h := hashKey(key)
	idx := sort.Search(len(r.hashRing), func(i int) bool {
		return r.hashRing[i] >= h
	})

	// Wrap around to start of ring if beyond the highest point
	if idx == len(r.hashRing) {
		idx = 0
	}

	nodeID, exists := r.nodeMap[r.hashRing[idx]]
	return nodeID, exists
}

// OwnsShard checks if the given node ID owns a specific environment shard
func (r *Ring) OwnsShard(nodeID string, envID string, shardID int) bool {
	shardKey := fmt.Sprintf("%s:shard:%d", envID, shardID)
	owner, exists := r.GetOwner(shardKey)
	if !exists {
		return false
	}
	return owner == nodeID
}

// GetOwnedShards returns the slice of shard IDs that the given node ID currently owns
func (r *Ring) GetOwnedShards(nodeID string, envID string, shardCount int) []int {
	if shardCount <= 0 {
		shardCount = constants.DefaultShardCount
	}
	owned := make([]int, 0)
	for s := 0; s < shardCount; s++ {
		if r.OwnsShard(nodeID, envID, s) {
			owned = append(owned, s)
		}
	}
	return owned
}

// Members returns the list of active members in the ring
func (r *Ring) Members() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	members := make([]string, 0, len(r.members))
	for m := range r.members {
		members = append(members, m)
	}
	sort.Strings(members)
	return members
}
