// Copyright 2026 BlaCkinkGJ
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

// Resolve returns the node address responsible for group.
func (r *Ring) Resolve(group string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.ring) == 0 {
		return ""
	}
	h := murmur3.Sum32([]byte(group))
	idx := sort.Search(len(r.ring), func(i int) bool { return r.ring[i] >= h })
	if idx == len(r.ring) {
		idx = 0
	}
	return r.nodeMap[r.ring[idx]]
}
