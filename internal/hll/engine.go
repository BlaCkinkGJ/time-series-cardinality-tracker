// internal/hll/engine.go
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
func (e *Engine) Add(seriesID string, value []byte) {
	e.mu.Lock()
	defer e.mu.Unlock()
	h, ok := e.hlls[seriesID]
	if !ok {
		h = New()
		e.hlls[seriesID] = h
	}
	h.Add(value)
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

// Range calls fn for every series. fn must not call Engine methods (deadlock).
func (e *Engine) Range(fn func(id string, h *HLL)) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	for id, h := range e.hlls {
		fn(id, h)
	}
}
