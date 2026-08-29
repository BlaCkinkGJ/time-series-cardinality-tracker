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
// Each group holds the algorithm's sketch serialised to bytes; the
// sketch is rehydrated via the registered Algorithm on demand.
//
// Engine is safe for concurrent use.
type Engine struct {
	mu     sync.RWMutex
	alg    Algorithm
	groups map[string][]byte
}

// NewEngine returns an Engine backed by alg. All groups stored in
// this engine use alg; Merge enforces sketch.AlgoName == alg.Name().
func NewEngine(alg Algorithm) *Engine {
	return &Engine{alg: alg, groups: make(map[string][]byte)}
}

// Add inserts id into group's sketch, creating the group if absent.
func (e *Engine) Add(group string, id uint64) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	b, ok := e.groups[group]
	if !ok {
		sk := e.alg.New()
		sk.Add(id)
		e.groups[group] = sk.Bytes()
		return nil
	}
	sk, err := e.alg.Parse(b)
	if err != nil {
		return fmt.Errorf("cardinality: parse %q: %w", group, err)
	}
	sk.Add(id)
	e.groups[group] = sk.Bytes()
	return nil
}

// Cardinality returns the count of unique ids in group.
// Returns ErrUnknownGroup if group is not present.
func (e *Engine) Cardinality(group string) (uint64, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	b, ok := e.groups[group]
	if !ok {
		return 0, fmt.Errorf("%w: %q", ErrUnknownGroup, group)
	}
	sk, err := e.alg.Parse(b)
	if err != nil {
		return 0, fmt.Errorf("cardinality: parse %q: %w", group, err)
	}
	return sk.Cardinality(), nil
}

// Merge unions remote into group's sketch. Returns ErrAlgoMismatch
// if remote's AlgoName does not match this engine's algorithm.
// If the group does not exist, remote's bytes are adopted as-is.
func (e *Engine) Merge(group string, remote Sketch) error {
	if remote == nil {
		return errors.New("cardinality: nil remote sketch")
	}
	if remote.AlgoName() != e.alg.Name() {
		return fmt.Errorf("%w: engine uses %q, got %q", ErrAlgoMismatch, e.alg.Name(), remote.AlgoName())
	}
	e.mu.Lock()
	defer e.mu.Unlock()

	b, ok := e.groups[group]
	if !ok {
		bb := remote.Bytes()
		if bb == nil {
			return errors.New("cardinality: new-group Merge with no bytes")
		}
		e.groups[group] = bb
		return nil
	}
	sk, err := e.alg.Parse(b)
	if err != nil {
		return fmt.Errorf("cardinality: parse %q: %w", group, err)
	}
	sk.Merge(remote)
	e.groups[group] = sk.Bytes()
	return nil
}

// Marshal serialises the full state using gob. The output is suitable
// for snapshots and round-trips through Unmarshal on an Engine with
// the same algorithm.
func (e *Engine) Marshal() ([]byte, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(e.groups); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Unmarshal replaces all state from data produced by Marshal.
// The data must have been produced by an Engine with the same
// algorithm; mismatched bytes fail to Parse and surface as an error.
func (e *Engine) Unmarshal(data []byte) error {
	var m map[string][]byte
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&m); err != nil {
		return err
	}
	for g, b := range m {
		if _, err := e.alg.Parse(b); err != nil {
			return fmt.Errorf("cardinality: parse %q: %w", g, err)
		}
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.groups = m
	return nil
}

// Range calls fn for each group, reconstructing the sketch from
// its bytes. If fn returns an error, Range stops and returns that error.
func (e *Engine) Range(fn func(group string, sk Sketch) error) error {
	if fn == nil {
		return errors.New("cardinality: nil range function")
	}
	e.mu.RLock()
	type pair struct {
		g   string
		buf []byte
	}
	snap := make([]pair, 0, len(e.groups))
	for g, b := range e.groups {
		snap = append(snap, pair{g: g, buf: b})
	}
	e.mu.RUnlock()

	for _, p := range snap {
		sk, err := e.alg.Parse(p.buf)
		if err != nil {
			return fmt.Errorf("cardinality: parse %q: %w", p.g, err)
		}
		if err := fn(p.g, sk); err != nil {
			return err
		}
	}
	return nil
}
