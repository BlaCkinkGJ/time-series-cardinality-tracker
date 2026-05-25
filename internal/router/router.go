package router

import (
	"fmt"
	"sort"
	"sync"

	"github.com/spaolacci/murmur3"
)

const vnodesPerNode = 150

// Ring is a thread-safe consistent hash ring.
type Ring struct {
	mu      sync.RWMutex
	ring    []uint32          // sorted hash points
	nodeMap map[uint32]string // hash → node address
}

func New() *Ring { return &Ring{nodeMap: make(map[uint32]string)} }

// AddNode adds a node with vnodesPerNode virtual nodes.
func (r *Ring) AddNode(addr string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := 0; i < vnodesPerNode; i++ {
		h := murmur3.Sum32([]byte(fmt.Sprintf("%s#%d", addr, i)))
		r.ring = append(r.ring, h)
		r.nodeMap[h] = addr
	}
	sort.Slice(r.ring, func(i, j int) bool { return r.ring[i] < r.ring[j] })
}

// RemoveNode removes all virtual nodes for addr.
func (r *Ring) RemoveNode(addr string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var keep []uint32
	for _, h := range r.ring {
		if r.nodeMap[h] != addr {
			keep = append(keep, h)
		} else {
			delete(r.nodeMap, h)
		}
	}
	r.ring = keep
}

// Resolve returns the node address responsible for seriesID.
func (r *Ring) Resolve(seriesID string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.ring) == 0 {
		return ""
	}
	h := murmur3.Sum32([]byte(seriesID))
	idx := sort.Search(len(r.ring), func(i int) bool { return r.ring[i] >= h })
	if idx == len(r.ring) {
		idx = 0
	}
	return r.nodeMap[r.ring[idx]]
}
