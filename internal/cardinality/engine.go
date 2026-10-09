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

package cardinality

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"sync"
)

// ErrUnknownGroup is returned by Cardinality when group is not in the engine.
var ErrUnknownGroup = errors.New("cardinality: unknown group")

// ErrAlgoMismatch is returned by Merge when the remote sketch's
// AlgoName does not match this engine's algorithm.
var ErrAlgoMismatch = errors.New("cardinality: sketch algo does not match engine")

// Engine is a per-group cardinality store for a single algorithm.
// Each group holds a live Sketch; bytes are only materialised for
// snapshots via Marshal.
//
// Engine is safe for concurrent use.
type Engine struct {
	mu     sync.RWMutex
	alg    Algorithm
	groups map[string]Sketch
}

// NewEngine returns an Engine backed by alg. All groups stored in
// this engine use alg; Merge enforces sketch.AlgoName == alg.Name().
func NewEngine(alg Algorithm) *Engine {
	return &Engine{alg: alg, groups: make(map[string]Sketch)}
}

// Add inserts id into group's sketch, creating the group if absent.
func (e *Engine) Add(group string, id uint64) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	sk, ok := e.groups[group]
	if !ok {
		sk = e.alg.New()
		e.groups[group] = sk
	}
	sk.Add(id)
	return nil
}

// Cardinality returns the count of unique ids in group.
// Returns ErrUnknownGroup if group is not present.
func (e *Engine) Cardinality(group string) (uint64, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	sk, ok := e.groups[group]
	if !ok {
		return 0, fmt.Errorf("%w: %q", ErrUnknownGroup, group)
	}
	return sk.Cardinality(), nil
}

// CardinalityAll returns the current estimate for every group, taken
// under a single read lock. It is the read path for the metrics
// collector, which needs all groups at once.
func (e *Engine) CardinalityAll() map[string]uint64 {
	e.mu.RLock()
	defer e.mu.RUnlock()

	out := make(map[string]uint64, len(e.groups))
	for group, sk := range e.groups {
		out[group] = sk.Cardinality()
	}
	return out
}

// AddAndPersist inserts id into group and, still holding the write lock,
// passes the freshly serialised sketch to save. Insert and persist are
// one atomic step: a concurrent add cannot have its newer snapshot
// overwritten by an older one, so every acknowledged add is in the
// persisted sketch.
//
// Trade-off: save runs under the engine-wide write lock, so standalone
// writes are serialised engine-wide. Per-group locks if that throughput
// ever matters.
func (e *Engine) AddAndPersist(group string, id uint64, save func([]byte) error) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	sk, ok := e.groups[group]
	if !ok {
		sk = e.alg.New()
		e.groups[group] = sk
	}
	sk.Add(id)
	return save(sk.Bytes())
}

// Persist serialises group's sketch and passes it to save while holding
// the write lock, so the bytes handed out are exactly the state at that
// moment and no older snapshot can be written after a newer one.
// Returns ErrUnknownGroup if group is not present.
func (e *Engine) Persist(group string, save func([]byte) error) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	sk, ok := e.groups[group]
	if !ok {
		return fmt.Errorf("%w: %q", ErrUnknownGroup, group)
	}
	return save(sk.Bytes())
}

// Restore parses b with the engine's algorithm and installs the result as
// group's sketch, replacing any existing one. It is the counterpart of
// Persist and the startup path that rebuilds state from the store: every
// applied entry was persisted, so the stored bytes are the authoritative
// state. Re-applying an id after a restart is idempotent, which is why
// replaying from an empty Raft log is safe.
func (e *Engine) Restore(group string, b []byte) error {
	sk, err := e.alg.Parse(b)
	if err != nil {
		return fmt.Errorf("cardinality: parse %q: %w", group, err)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.groups[group] = sk
	return nil
}

// Merge unions remote into group's sketch. Returns ErrAlgoMismatch
// if remote's AlgoName does not match this engine's algorithm.
// If the group does not exist, remote is cloned into the engine so
// later mutations of remote do not affect engine state.
func (e *Engine) Merge(group string, remote Sketch) error {
	if remote == nil {
		return errors.New("cardinality: nil remote sketch")
	}
	if remote.AlgoName() != e.alg.Name() {
		return fmt.Errorf("%w: engine uses %q, got %q", ErrAlgoMismatch, e.alg.Name(), remote.AlgoName())
	}
	e.mu.Lock()
	defer e.mu.Unlock()

	sk, ok := e.groups[group]
	if !ok {
		e.groups[group] = remote.Clone()
		return nil
	}
	sk.Merge(remote)
	return nil
}

// ValidateSketch checks algoName and parses b with the engine's algorithm
// without touching engine state. The write path uses it to reject a bad merge
// *before* it becomes a log entry: once proposed, the entry is accepted
// regardless of whether the apply it later triggers succeeds, so anything
// wrong with the payload has to fail here to reach the caller.
func (e *Engine) ValidateSketch(algoName string, b []byte) error {
	if algoName != e.alg.Name() {
		return fmt.Errorf("%w: engine uses %q, got %q", ErrAlgoMismatch, e.alg.Name(), algoName)
	}
	if _, err := e.alg.Parse(b); err != nil {
		return fmt.Errorf("cardinality: parse %q: %w", algoName, err)
	}
	return nil
}

// MergeBytes parses b with the engine's algorithm and unions the
// result into group. algoName must match the engine's algorithm;
// a mismatch (config drift between nodes) returns ErrAlgoMismatch.
// This is the wire-facing entry point used by the MERGE_SKETCH handler.
func (e *Engine) MergeBytes(group, algoName string, b []byte) error {
	if algoName != e.alg.Name() {
		return fmt.Errorf("%w: engine uses %q, got %q", ErrAlgoMismatch, e.alg.Name(), algoName)
	}
	remote, err := e.alg.Parse(b)
	if err != nil {
		return fmt.Errorf("cardinality: parse %q: %w", group, err)
	}
	return e.Merge(group, remote)
}

// Marshal serialises the live sketches to a gob-encoded
// map[string][]byte snapshot. Round-trips through Unmarshal on an
// Engine with the same algorithm.
func (e *Engine) Marshal() ([]byte, error) {
	e.mu.RLock()
	wire := make(map[string][]byte, len(e.groups))
	for g, sk := range e.groups {
		wire[g] = sk.Bytes()
	}
	e.mu.RUnlock()

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(wire); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Unmarshal replaces all state from data produced by Marshal.
// Each group's bytes are Parsed into a fresh Sketch; a Parse
// failure aborts before any state is replaced.
func (e *Engine) Unmarshal(data []byte) error {
	var wire map[string][]byte
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&wire); err != nil {
		return err
	}
	groups := make(map[string]Sketch, len(wire))
	for g, b := range wire {
		sk, err := e.alg.Parse(b)
		if err != nil {
			return fmt.Errorf("cardinality: parse %q: %w", g, err)
		}
		groups[g] = sk
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.groups = groups
	return nil
}
