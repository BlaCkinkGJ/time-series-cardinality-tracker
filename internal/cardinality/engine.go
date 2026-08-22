package cardinality

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"reflect"
	"sync"
)

// ErrUnknownGroup is returned by Cardinality when group is not in the engine.
var ErrUnknownGroup = errors.New("cardinality: unknown group")

// ErrUnknownAlgo is returned by Add when algo is not registered.
var ErrUnknownAlgo = errors.New("cardinality: unknown algorithm")

// ErrAlgoMismatch is returned by Merge when the remote sketch cannot
// be applied to the group's existing algo.
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
// algorithm registered under that name.
//
// Engine is safe for concurrent use.
type Engine struct {
	mu     sync.RWMutex
	groups map[string]Entry
}

// NewEngine returns an empty Engine.
func NewEngine() *Engine {
	return &Engine{groups: make(map[string]Entry)}
}

// Add inserts id into group, creating the group with algo if absent.
// If the group already exists, the existing algo is reused and the
// new algo parameter is ignored.
func (e *Engine) Add(group, algo string, id uint64) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	entry, ok := e.groups[group]
	if !ok {
		alg, found := Get(algo)
		if !found {
			return fmt.Errorf("%w: %q", ErrUnknownAlgo, algo)
		}
		sk := alg.New()
		sk.Add(id)
		e.groups[group] = Entry{Algo: algo, Bytes: sk.Bytes()}
		return nil
	}

	alg, found := Get(entry.Algo)
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
	alg, found := Get(entry.Algo)
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
// If the group does not exist, the remote sketch is adopted and a
// new group is created with the remote's algo (derived from the
// factory registered under group; callers wanting to seed with a
// pre-built sketch should use Parse-then-bytes when needed).
//
// Returns ErrAlgoMismatch if remote cannot be applied to the
// group's algo (e.g. a *Bitmap sketch merged into a non-bitmap group).
func (e *Engine) Merge(group string, remote Sketch) error {
	if remote == nil {
		return errors.New("cardinality: nil remote sketch")
	}
	e.mu.Lock()
	defer e.mu.Unlock()

	entry, ok := e.groups[group]
	if !ok {
		// New group: infer algo from the remote's concrete type.
		name := findAlgoName(remote)
		if name == "" {
			return errors.New("cardinality: cannot determine algo for new-group Merge")
		}
		b := remote.Bytes()
		if b == nil {
			return errors.New("cardinality: new-group Merge with no bytes")
		}
		e.groups[group] = Entry{Algo: name, Bytes: b}
		return nil
	}

	alg, found := Get(entry.Algo)
	if !found {
		return fmt.Errorf("%w: %q (group %q)", ErrUnknownAlgo, entry.Algo, group)
	}
	sk, err := alg.Parse(entry.Bytes)
	if err != nil {
		return fmt.Errorf("cardinality: parse %q: %w", group, err)
	}
	// Verify remote matches group's algo by type, so a *Bitmap can't
	// be merged into a non-bitmap group (or vice versa).
	remoteName := findAlgoName(remote)
	if remoteName != entry.Algo {
		return fmt.Errorf("%w: group %q uses %q, got %q", ErrAlgoMismatch, group, entry.Algo, remoteName)
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
func (e *Engine) Unmarshal(data []byte) error {
	var m map[string]Entry
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&m); err != nil {
		return err
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
		alg, found := Get(p.ent.Algo)
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

// findAlgoName returns the registered algo whose New() produces a
// sketch of the same dynamic type as remote, or "" if no match.
// ponytail: O(N) registry scan on every Merge; for the foundation
// slice N is tiny (1-3 algos). If a hot path emerges, cache a
// reflect.Type -> name map.
func findAlgoName(remote Sketch) string {
	rt := reflect.TypeOf(remote)
	if rt == nil {
		return ""
	}
	for name, alg := range registry {
		if reflect.TypeOf(alg.New()) == rt {
			return name
		}
	}
	return ""
}
