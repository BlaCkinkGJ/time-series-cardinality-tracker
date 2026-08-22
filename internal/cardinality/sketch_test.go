package cardinality

import (
	"errors"
	"testing"
)

// fakeSketch is a minimal Sketch implementation for testing the
// interface contract and registry. It is unexported because tests
// for the registry and Engine can use the registered "fake" algo.
type fakeSketch struct {
	data map[uint64]struct{}
}

func newFakeSketch() *fakeSketch {
	return &fakeSketch{data: make(map[uint64]struct{})}
}

func (f *fakeSketch) Add(id uint64)       { f.data[id] = struct{}{} }
func (f *fakeSketch) Cardinality() uint64 { return uint64(len(f.data)) }
func (f *fakeSketch) Merge(s Sketch) {
	other, ok := s.(*fakeSketch)
	if !ok {
		return
	}
	for k := range other.data {
		f.data[k] = struct{}{}
	}
}

// Bytes: big-endian 8 bytes per id.
func (f *fakeSketch) Bytes() []byte {
	out := make([]byte, 0, len(f.data)*8)
	for k := range f.data {
		out = append(out,
			byte(k>>56), byte(k>>48), byte(k>>40), byte(k>>32),
			byte(k>>24), byte(k>>16), byte(k>>8), byte(k))
	}
	return out
}

func (f *fakeSketch) Clone() Sketch {
	c := newFakeSketch()
	for k := range f.data {
		c.data[k] = struct{}{}
	}
	return c
}

// fakeAlgorithm is registered under "fake" for tests.
type fakeAlgorithm struct{}

const fakeAlgoName = "fake"

func (fakeAlgorithm) Name() string { return fakeAlgoName }
func (fakeAlgorithm) New() Sketch  { return newFakeSketch() }
func (fakeAlgorithm) Parse(b []byte) (Sketch, error) {
	if len(b)%8 != 0 {
		return nil, errors.New("fakeAlgorithm: invalid byte length")
	}
	s := newFakeSketch()
	for i := 0; i < len(b); i += 8 {
		id := uint64(b[i])<<56 | uint64(b[i+1])<<48 | uint64(b[i+2])<<40 | uint64(b[i+3])<<32 |
			uint64(b[i+4])<<24 | uint64(b[i+5])<<16 | uint64(b[i+6])<<8 | uint64(b[i+7])
		s.Add(id)
	}
	return s, nil
}

// setupTest resets the registry and registers the fake algorithm.
func setupTest(t *testing.T) {
	t.Helper()
	resetRegistry()
	Register(fakeAlgorithm{})
}

func TestSketch_AddCardinality(t *testing.T) {
	setupTest(t)
	algo, ok := Get(fakeAlgoName)
	if !ok {
		t.Fatal("fake algorithm not registered")
	}
	sk := algo.New()
	for i := uint64(0); i < 10; i++ {
		sk.Add(i)
	}
	if got := sk.Cardinality(); got != 10 {
		t.Fatalf("cardinality = %d, want 10", got)
	}
}

func TestSketch_MergeCommutative(t *testing.T) {
	setupTest(t)
	algo, _ := Get(fakeAlgoName)

	a := algo.New()
	a.Add(1)
	a.Add(2)
	a.Add(3)
	b := algo.New()
	b.Add(3)
	b.Add(4)
	b.Add(5)

	ab := a.Clone()
	ab.Merge(b)

	ba := b.Clone()
	ba.Merge(a)

	if ab.Cardinality() != ba.Cardinality() {
		t.Fatalf("not commutative: a|b=%d b|a=%d", ab.Cardinality(), ba.Cardinality())
	}
	if got := ab.Cardinality(); got != 5 {
		t.Fatalf("merged cardinality = %d, want 5", got)
	}
}

func TestSketch_BytesParseRoundtrip(t *testing.T) {
	setupTest(t)
	algo, _ := Get(fakeAlgoName)
	sk := algo.New()
	sk.Add(1)
	sk.Add(42)
	sk.Add(1000)
	sk.Add(1 << 40)

	parsed, err := algo.Parse(sk.Bytes())
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if parsed.Cardinality() != sk.Cardinality() {
		t.Fatalf("roundtrip: parsed=%d original=%d", parsed.Cardinality(), sk.Cardinality())
	}
}

func TestRegistry_RegisterGet(t *testing.T) {
	setupTest(t)
	algo, ok := Get(fakeAlgoName)
	if !ok {
		t.Fatal("fake algorithm not registered")
	}
	if algo.Name() != fakeAlgoName {
		t.Fatalf("name = %q, want %q", algo.Name(), fakeAlgoName)
	}
}

func TestRegistry_GetUnknown(t *testing.T) {
	setupTest(t)
	_, ok := Get("does-not-exist")
	if ok {
		t.Fatal("Get returned ok for unknown algorithm")
	}
}

func TestRegistry_Duplicate(t *testing.T) {
	setupTest(t)
	// Last-wins policy: re-registering the same name must not panic,
	// and Get must still return an algorithm with that name.
	Register(fakeAlgorithm{})
	algo, ok := Get(fakeAlgoName)
	if !ok {
		t.Fatal("fake algorithm lost after re-register")
	}
	if algo.Name() != fakeAlgoName {
		t.Fatalf("name = %q, want %q", algo.Name(), fakeAlgoName)
	}
}
