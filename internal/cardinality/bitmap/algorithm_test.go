package bitmap

import (
	"bytes"
	"testing"
)

func TestAlgorithm_Name(t *testing.T) {
	if got := (Algorithm{}).Name(); got != algoName {
		t.Fatalf("Name = %q, want %q", got, algoName)
	}
}

func TestAlgorithm_New(t *testing.T) {
	sk := (Algorithm{}).New()
	if sk == nil {
		t.Fatal("New returned nil")
	}
	if got := sk.Cardinality(); got != 0 {
		t.Fatalf("empty cardinality = %d, want 0", got)
	}
	if got := sk.AlgoName(); got != algoName {
		t.Fatalf("AlgoName = %q, want %q", got, algoName)
	}
}

func TestAlgorithm_Parse(t *testing.T) {
	alg := Algorithm{}
	src := alg.New()
	src.Add(1)
	src.Add(42)
	src.Add(1000)

	parsed, err := alg.Parse(src.Bytes())
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if parsed.Cardinality() != 3 {
		t.Fatalf("cardinality = %d, want 3", parsed.Cardinality())
	}
	if parsed.AlgoName() != algoName {
		t.Fatalf("AlgoName = %q, want %q", parsed.AlgoName(), algoName)
	}
}

// TestSketch_MarshalEmpty verifies an empty sketch round-trips
// without error (catches nil-byte bugs).
func TestSketch_MarshalEmpty(t *testing.T) {
	sk := (Algorithm{}).New()
	data := sk.Bytes()
	if len(data) == 0 {
		t.Fatal("Bytes of empty sketch returned empty slice")
	}
	parsed, err := (Algorithm{}).Parse(data)
	if err != nil {
		t.Fatalf("Parse empty: %v", err)
	}
	if parsed.Cardinality() != 0 {
		t.Fatalf("cardinality = %d, want 0", parsed.Cardinality())
	}
}

// TestSketch_ParseCorrupt verifies Parse rejects malformed bytes.
func TestSketch_ParseCorrupt(t *testing.T) {
	if _, err := (Algorithm{}).Parse(bytes.Repeat([]byte{0xff}, 4)); err == nil {
		t.Fatal("expected error on corrupt bytes, got nil")
	}
}

// TestSketch_Merge verifies sketch.Merge unions data.
func TestSketch_Merge(t *testing.T) {
	alg := Algorithm{}
	a := alg.New()
	a.Add(1)
	a.Add(2)
	a.Add(3)
	b := alg.New()
	b.Add(3)
	b.Add(4)
	b.Add(5)

	a.Merge(b)

	if got := a.Cardinality(); got != 5 {
		t.Fatalf("merged cardinality = %d, want 5", got)
	}
}

// TestSketch_Clone_Deep verifies Clone returns an independent copy.
func TestSketch_Clone_Deep(t *testing.T) {
	alg := Algorithm{}
	a := alg.New()
	a.Add(1)
	a.Add(2)
	a.Add(3)

	clone := a.Clone().(*sketch)
	clone.Add(99)

	if a.Cardinality() != 3 {
		t.Fatalf("original changed after clone mutation: %d", a.Cardinality())
	}
	if clone.Cardinality() != 4 {
		t.Fatalf("clone cardinality = %d, want 4", clone.Cardinality())
	}
}

// TestSketch_AlgoName_MatchesAlgorithm ensures the sketch self-reports
// the same name the factory uses.
func TestSketch_AlgoName_MatchesAlgorithm(t *testing.T) {
	alg := Algorithm{}
	sk := alg.New()
	if sk.AlgoName() != alg.Name() {
		t.Fatalf("sketch.AlgoName()=%q != alg.Name()=%q", sk.AlgoName(), alg.Name())
	}
}
