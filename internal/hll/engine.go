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

package hll

import "sync"

// Engine is a thread-safe map of series ID to HLL sketches.
type Engine struct {
	mu   sync.RWMutex
	hlls map[string]*HLL
}

// NewEngine returns an empty Engine.
func NewEngine() *Engine { return &Engine{hlls: make(map[string]*HLL)} }

// Add inserts value into the HLL for seriesID (creates entry if absent).
func (e *Engine) Add(seriesID, value string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	h, ok := e.hlls[seriesID]
	if !ok {
		h = New()
		e.hlls[seriesID] = h
	}
	h.Add([]byte(value))
}

// Estimate returns the cardinality estimate for seriesID (0 if unknown).
func (e *Engine) Estimate(seriesID string) uint64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if h, ok := e.hlls[seriesID]; ok {
		return h.Estimate()
	}
	return 0
}

// Get returns the HLL for seriesID and whether it exists.
func (e *Engine) Get(seriesID string) (*HLL, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	h, ok := e.hlls[seriesID]
	return h, ok
}

// Set replaces the HLL for seriesID.
func (e *Engine) Set(seriesID string, h *HLL) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.hlls[seriesID] = h
}

// Merge unions remote into the HLL for seriesID, creating the entry
// from remote if absent. Used by the raft MERGE_SKETCH handler.
func (e *Engine) Merge(seriesID string, remote *HLL) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if h, ok := e.hlls[seriesID]; ok {
		h.Merge(remote)
		return
	}
	e.hlls[seriesID] = remote
}

// Range calls fn for every series. fn must not call Engine methods (deadlock).
func (e *Engine) Range(fn func(id string, h *HLL)) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	for id, h := range e.hlls {
		fn(id, h)
	}
}
