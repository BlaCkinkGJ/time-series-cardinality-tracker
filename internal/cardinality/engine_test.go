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
	"sync"
	"testing"
)

// mismatchedSketch is a Sketch whose AlgoName() differs from fakeSketch's,
// used to verify Engine.Merge rejects cross-algo merges.
type mismatchedSketch struct{}

const mismatchedAlgoName = "mismatched"

func (mismatchedSketch) AlgoName() string    { return mismatchedAlgoName }
func (mismatchedSketch) Add(uint64)          {}
func (mismatchedSketch) Cardinality() uint64 { return 0 }
func (mismatchedSketch) Merge(Sketch)        {}
func (mismatchedSketch) Bytes() []byte       { return []byte{} }
func (mismatchedSketch) Clone() Sketch       { return mismatchedSketch{} }

// newTestEngine returns an Engine backed by fakeAlgorithm.
func newTestEngine(t *testing.T) *Engine {
	t.Helper()
	return NewEngine(fakeAlgorithm{})
}

func TestEngine_AddNew(t *testing.T) {
	e := newTestEngine(t)
	if err := e.Add("g1", 1); err != nil {
		t.Fatalf("Add: %v", err)
	}
	got, err := e.Cardinality("g1")
	if err != nil {
		t.Fatalf("Cardinality: %v", err)
	}
	if got != 1 {
		t.Fatalf("cardinality = %d, want 1", got)
	}
}

func TestEngine_AddExisting(t *testing.T) {
	e := newTestEngine(t)
	if err := e.Add("g1", 1); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := e.Add("g1", 2); err != nil {
		t.Fatalf("Add: %v", err)
	}
	got, err := e.Cardinality("g1")
	if err != nil {
		t.Fatalf("Cardinality: %v", err)
	}
	if got != 2 {
		t.Fatalf("cardinality = %d, want 2", got)
	}
}

func TestEngine_CardinalityUnknown(t *testing.T) {
	e := newTestEngine(t)
	_, err := e.Cardinality("missing")
	if err == nil {
		t.Fatal("expected error for unknown group, got nil")
	}
}

func TestEngine_MergeNewGroup(t *testing.T) {
	e := newTestEngine(t)
	sk := newFakeSketch()
	sk.Add(7)
	sk.Add(8)
	if err := e.Merge("g-new", sk); err != nil {
		t.Fatalf("Merge: %v", err)
	}
	got, err := e.Cardinality("g-new")
	if err != nil {
		t.Fatalf("Cardinality: %v", err)
	}
	if got != 2 {
		t.Fatalf("cardinality = %d, want 2", got)
	}
}

func TestEngine_MergeExisting(t *testing.T) {
	e := newTestEngine(t)
	if err := e.Add("g1", 1); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := e.Add("g1", 2); err != nil {
		t.Fatalf("Add: %v", err)
	}
	sk := newFakeSketch()
	sk.Add(2)
	sk.Add(3)
	if err := e.Merge("g1", sk); err != nil {
		t.Fatalf("Merge: %v", err)
	}
	got, _ := e.Cardinality("g1")
	if got != 3 {
		t.Fatalf("cardinality = %d, want 3", got)
	}
}

// TestEngine_MergeTypeMismatch verifies Merge rejects a remote whose
// AlgoName does not match the engine's algorithm.
func TestEngine_MergeTypeMismatch(t *testing.T) {
	e := newTestEngine(t)
	if err := e.Add("g1", 1); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := e.Merge("g1", mismatchedSketch{}); err == nil {
		t.Fatal("expected error for cross-algo Merge, got nil")
	}
}

func TestEngine_MarshalRoundtrip(t *testing.T) {
	e := newTestEngine(t)
	if err := e.Add("alpha", 1); err != nil {
		t.Fatalf("Add alpha: %v", err)
	}
	if err := e.Add("beta", 1); err != nil {
		t.Fatalf("Add beta: %v", err)
	}
	if err := e.Add("beta", 2); err != nil {
		t.Fatalf("Add beta: %v", err)
	}

	data, err := e.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("Marshal produced empty bytes")
	}

	e2 := NewEngine(fakeAlgorithm{})
	if err := e2.Unmarshal(data); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	for _, g := range []string{"alpha", "beta"} {
		a, err := e.Cardinality(g)
		if err != nil {
			t.Fatalf("Cardinality %s: %v", g, err)
		}
		b, err := e2.Cardinality(g)
		if err != nil {
			t.Fatalf("Cardinality %s (e2): %v", g, err)
		}
		if a != b {
			t.Fatalf("group %s: pre=%d post=%d", g, a, b)
		}
	}
}

func TestEngine_AddAndPersist(t *testing.T) {
	e := newTestEngine(t)
	var saved [][]byte
	save := func(b []byte) error {
		saved = append(saved, append([]byte(nil), b...))
		return nil
	}

	if err := e.AddAndPersist("g", 1, save); err != nil {
		t.Fatalf("AddAndPersist: %v", err)
	}
	if err := e.AddAndPersist("g", 2, save); err != nil {
		t.Fatalf("AddAndPersist: %v", err)
	}
	if len(saved) != 2 {
		t.Fatalf("save calls = %d, want 2", len(saved))
	}
	// Each persisted snapshot must already contain the id it accompanied.
	for i, want := range []uint64{1, 2} {
		sk, err := fakeAlgorithm{}.Parse(saved[i])
		if err != nil {
			t.Fatalf("parse save %d: %v", i, err)
		}
		if got := sk.Cardinality(); got != want {
			t.Fatalf("save %d cardinality = %d, want %d", i, got, want)
		}
	}
}

// TestEngine_AddAndPersist_ConcurrentSuperset pins the invariant that
// made AddAndPersist necessary: because save runs under the same lock as
// the insert, the last persisted snapshot is the superset containing
// every add. Releasing the lock before save lets an older snapshot win.
func TestEngine_AddAndPersist_ConcurrentSuperset(t *testing.T) {
	e := newTestEngine(t)
	const n = 128

	var mu sync.Mutex
	var last []byte
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(id uint64) {
			defer wg.Done()
			err := e.AddAndPersist("g", id, func(b []byte) error {
				mu.Lock()
				last = append(last[:0], b...)
				mu.Unlock()
				return nil
			})
			if err != nil {
				t.Errorf("AddAndPersist: %v", err)
			}
		}(uint64(i))
	}
	wg.Wait()

	sk, err := fakeAlgorithm{}.Parse(last)
	if err != nil {
		t.Fatalf("parse last save: %v", err)
	}
	if got := sk.Cardinality(); got != n {
		t.Fatalf("final persisted cardinality = %d, want %d", got, n)
	}
}

func TestEngine_ConcurrentAdd(t *testing.T) {
	e := newTestEngine(t)
	const goroutines = 16
	const perGoroutine = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		base := uint64(g) * perGoroutine
		go func() {
			defer wg.Done()
			for i := uint64(0); i < perGoroutine; i++ {
				if err := e.Add("shared", base+i); err != nil {
					t.Errorf("Add: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()

	got, err := e.Cardinality("shared")
	if err != nil {
		t.Fatalf("Cardinality: %v", err)
	}
	if want := uint64(goroutines * perGoroutine); got != want {
		t.Fatalf("cardinality = %d, want %d", got, want)
	}
}
