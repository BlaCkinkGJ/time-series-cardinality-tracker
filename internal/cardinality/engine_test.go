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

type mismatchedAlgorithm struct{}

func (mismatchedAlgorithm) Name() string { return mismatchedAlgoName }
func (mismatchedAlgorithm) New() Sketch  { return mismatchedSketch{} }
func (mismatchedAlgorithm) Parse([]byte) (Sketch, error) {
	return mismatchedSketch{}, nil
}

// newTestEngine returns an Engine with the "fake" algorithm registered.
func newTestEngine(t *testing.T) *Engine {
	t.Helper()
	return NewEngine(newTestAlgo())
}

func TestEngine_AddNew(t *testing.T) {
	e := newTestEngine(t)
	if err := e.Add("g1", fakeAlgoName, 1); err != nil {
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
	if err := e.Add("g1", fakeAlgoName, 1); err != nil {
		t.Fatalf("Add: %v", err)
	}
	// Subsequent adds reuse the group's algo.
	if err := e.Add("g1", fakeAlgoName, 2); err != nil {
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

func TestEngine_AddUnknownAlgo(t *testing.T) {
	e := newTestEngine(t)
	if err := e.Add("g1", "not-registered", 1); err == nil {
		t.Fatal("expected error for unknown algo, got nil")
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

func TestEngine_MergeNewGroupUnknownAlgo(t *testing.T) {
	e := newTestEngine(t)
	if err := e.Merge("g-new", mismatchedSketch{}); err == nil {
		t.Fatal("expected error for unregistered algo, got nil")
	}
}

func TestEngine_MergeExisting(t *testing.T) {
	e := newTestEngine(t)
	if err := e.Add("g1", fakeAlgoName, 1); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := e.Add("g1", fakeAlgoName, 2); err != nil {
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

// TestEngine_MergeTypeMismatch verifies that a Merge into a group
// carrying a different algo than the remote sketch returns an error,
// not a panic.
func TestEngine_MergeTypeMismatch(t *testing.T) {
	e := NewEngine(newTestAlgo(), mismatchedAlgorithm{})
	if err := e.Add("g1", fakeAlgoName, 1); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := e.Merge("g1", mismatchedSketch{}); err == nil {
		t.Fatal("expected error for cross-algo Merge, got nil")
	}
}

func TestEngine_MarshalRoundtrip(t *testing.T) {
	e := newTestEngine(t)
	if err := e.Add("alpha", fakeAlgoName, 1); err != nil {
		t.Fatalf("Add alpha: %v", err)
	}
	if err := e.Add("beta", fakeAlgoName, 1); err != nil {
		t.Fatalf("Add beta: %v", err)
	}
	if err := e.Add("beta", fakeAlgoName, 2); err != nil {
		t.Fatalf("Add beta: %v", err)
	}

	data, err := e.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("Marshal produced empty bytes")
	}

	e2 := NewEngine(newTestAlgo())
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

func TestEngine_Range_VisitsAll(t *testing.T) {
	e := newTestEngine(t)
	if err := e.Add("a", fakeAlgoName, 1); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := e.Add("b", fakeAlgoName, 1); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := e.Add("c", fakeAlgoName, 1); err != nil {
		t.Fatalf("Add: %v", err)
	}

	seen := map[string]uint64{}
	err := e.Range(func(group, algo string, sk Sketch) error {
		if algo != fakeAlgoName {
			t.Errorf("group %s: algo = %q, want %q", group, algo, fakeAlgoName)
		}
		if sk == nil {
			t.Errorf("group %s: nil sketch", group)
		}
		seen[group] = sk.Cardinality()
		return nil
	})
	if err != nil {
		t.Fatalf("Range: %v", err)
	}
	if len(seen) != 3 {
		t.Fatalf("Range visited %d groups, want 3", len(seen))
	}
	for _, g := range []string{"a", "b", "c"} {
		if _, ok := seen[g]; !ok {
			t.Fatalf("Range did not visit %q", g)
		}
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
				if err := e.Add("shared", fakeAlgoName, base+i); err != nil {
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
