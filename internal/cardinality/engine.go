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

// ErrUnknownAlgo is returned by Add when algo is not registered on the engine.
var ErrUnknownAlgo = errors.New("cardinality: unknown algorithm")

// ErrAlgoMismatch is returned by Merge when the remote sketch's AlgoName
// does not match the group's existing algo.
var ErrAlgoMismatch = errors.New("cardinality: sketch algo does not match group")

// Entry holds the persisted state of one group: the algorithm name
// and the sketch's serialised bytes. Bytes are stored opaque; the
// engine uses the registered Algorithm to Parse them on demand.
type Entry struct {
	Algo  string
	Bytes []byte
}

// Engine is a per-group cardinality store. Each group carries
// (algo, bytes) and a sketch is reconstructed on demand via the
// algorithm registered on this engine.
//
// Engine is safe for concurrent use.
type Engine struct {
	mu     sync.RWMutex
	groups map[string]Entry
	algos  map[string]Algorithm
}

// NewEngine returns an Engine that supports the given algorithms.
// At least one algorithm is required to Add or Unmarshal; an empty
// engine is useful only for inspecting pre-marshalled state via
// Cardinality/Range when the right algo is added later.
func NewEngine(algos ...Algorithm) *Engine {
	m := make(map[string]Algorithm, len(algos))
	for _, a := range algos {
		m[a.Name()] = a
	}
	return &Engine{groups: make(map[string]Entry), algos: m}
}

// Add inserts id into group, creating the group with algo if absent.
// If the group already exists, the existing algo is reused and the
// new algo parameter is ignored.
func (e *Engine) Add(group, algo string, id uint64) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	entry, ok := e.groups[group]
	if !ok {
		alg, found := e.algos[algo]
		if !found {
			return fmt.Errorf("%w: %q", ErrUnknownAlgo, algo)
		}
		sk := alg.New()
		sk.Add(id)
		e.groups[group] = Entry{Algo: algo, Bytes: sk.Bytes()}
		return nil
	}

	alg, found := e.algos[entry.Algo]
	if !found {
		return fmt.Errorf("%w: %q (group %q)", ErrUnknownAlgo, entry.Algo, group)
	}
	sk, err := alg.Parse(entry.Bytes)
	if err != nil {
		return fmt.Errorf("cardinality: parse %q: %w", group, err)
	}
	sk.Add(id)
	entry.Bytes = sk.Bytes()
	e.groups[group] = entry
	return nil
}

// Cardinality returns the count of unique ids in group.
// Returns ErrUnknownGroup if group is not present.
func (e *Engine) Cardinality(group string) (uint64, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	entry, ok := e.groups[group]
	if !ok {
		return 0, fmt.Errorf("%w: %q", ErrUnknownGroup, group)
	}
	alg, found := e.algos[entry.Algo]
	if !found {
		return 0, fmt.Errorf("%w: %q (group %q)", ErrUnknownAlgo, entry.Algo, group)
	}
	sk, err := alg.Parse(entry.Bytes)
	if err != nil {
		return 0, fmt.Errorf("cardinality: parse %q: %w", group, err)
	}
	return sk.Cardinality(), nil
}

// Merge unions remote into group's sketch, using the group's algo.
// If the group does not exist, the remote sketch is adopted as-is
// under its AlgoName.
//
// Returns ErrAlgoMismatch if remote's AlgoName does not match the
// group's existing algo.
func (e *Engine) Merge(group string, remote Sketch) error {
	if remote == nil {
		return errors.New("cardinality: nil remote sketch")
	}
	remoteName := remote.AlgoName()
	e.mu.Lock()
	defer e.mu.Unlock()

	entry, ok := e.groups[group]
	if !ok {
		if _, found := e.algos[remoteName]; !found {
			return fmt.Errorf("%w: %q", ErrUnknownAlgo, remoteName)
		}
		b := remote.Bytes()
		if b == nil {
			return errors.New("cardinality: new-group Merge with no bytes")
		}
		e.groups[group] = Entry{Algo: remoteName, Bytes: b}
		return nil
	}

	if remoteName != entry.Algo {
		return fmt.Errorf("%w: group %q uses %q, got %q", ErrAlgoMismatch, group, entry.Algo, remoteName)
	}
	alg, found := e.algos[entry.Algo]
	if !found {
		return fmt.Errorf("%w: %q (group %q)", ErrUnknownAlgo, entry.Algo, group)
	}
	sk, err := alg.Parse(entry.Bytes)
	if err != nil {
		return fmt.Errorf("cardinality: parse %q: %w", group, err)
	}
	sk.Merge(remote)
	entry.Bytes = sk.Bytes()
	e.groups[group] = entry
	return nil
}

// Marshal serialises the full state using gob. The output is suitable
// for snapshots and round-trips through Unmarshal.
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
// Each group's algo must be registered on the engine.
func (e *Engine) Unmarshal(data []byte) error {
	var m map[string]Entry
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&m); err != nil {
		return err
	}
	for g, ent := range m {
		if _, ok := e.algos[ent.Algo]; !ok {
			return fmt.Errorf("%w: %q (group %q)", ErrUnknownAlgo, ent.Algo, g)
		}
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.groups = m
	return nil
}

// Range calls fn for each group, reconstructing the sketch from
// its bytes via the registered algorithm. If fn returns an error,
// Range stops and returns that error.
func (e *Engine) Range(fn func(group, algo string, sk Sketch) error) error {
	if fn == nil {
		return errors.New("cardinality: nil range function")
	}
	e.mu.RLock()
	type pair struct {
		g   string
		ent Entry
	}
	snap := make([]pair, 0, len(e.groups))
	for g, ent := range e.groups {
		snap = append(snap, pair{g: g, ent: ent})
	}
	e.mu.RUnlock()

	for _, p := range snap {
		alg, found := e.algos[p.ent.Algo]
		if !found {
			return fmt.Errorf("%w: %q (group %q)", ErrUnknownAlgo, p.ent.Algo, p.g)
		}
		sk, err := alg.Parse(p.ent.Bytes)
		if err != nil {
			return fmt.Errorf("cardinality: parse %q: %w", p.g, err)
		}
		if err := fn(p.g, p.ent.Algo, sk); err != nil {
			return err
		}
	}
	return nil
}
