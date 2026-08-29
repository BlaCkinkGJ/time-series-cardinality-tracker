package bitmap

import "testing"

func TestAlgorithm_Name(t *testing.T) {
	a := Algorithm{}
	if got := a.Name(); got != algoName {
		t.Fatalf("Name = %q, want %q", got, algoName)
	}
}

func TestAlgorithm_New(t *testing.T) {
	a := Algorithm{}
	sk := a.New()
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
	// Build a sketch with a few ids, marshal it, then parse via Algorithm.
	src := New()
	src.Add(1)
	src.Add(42)
	src.Add(1000)
	data, err := src.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	a := Algorithm{}
	parsed, err := a.Parse(data)
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
