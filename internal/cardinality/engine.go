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

// AddAndPersist inserts id into group and, still holding the write lock,
// passes the freshly serialised sketch to save. Insert and persist are
// one atomic step: a concurrent add cannot have its newer snapshot
// overwritten by an older one, so every acknowledged add is in the
// persisted sketch.
//
// ponytail: save runs under the engine-wide write lock, serialising all
// standalone writes. Per-group locks if standalone write throughput matters.
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

// Bytes returns the serialised sketch for group, or ErrUnknownGroup.
// It is a point-in-time read: a concurrent Add may land immediately
// after it returns. Used only by the raft apply path, which is the sole
// writer in that mode and therefore cannot observe a reordering.
func (e *Engine) Bytes(group string) ([]byte, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	sk, ok := e.groups[group]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrUnknownGroup, group)
	}
	return sk.Bytes(), nil
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
