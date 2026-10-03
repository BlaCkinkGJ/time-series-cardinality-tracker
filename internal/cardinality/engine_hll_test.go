package cardinality_test

import (
	"errors"
	"testing"

	"github.com/yourorg/cardinality-tracker/internal/cardinality"
	"github.com/yourorg/cardinality-tracker/internal/cardinality/hll"
)

// TestEngine_HLLRoundtrip exercises the HLL implementation through the
// Engine: add, estimate, snapshot, restore.
func TestEngine_HLLRoundtrip(t *testing.T) {
	eng := cardinality.NewEngine(hll.Algorithm{})
	const n = 1000
	for i := 0; i < n; i++ {
		if err := eng.Add("g", uint64(i)); err != nil {
			t.Fatal(err)
		}
	}
	got, err := eng.Cardinality("g")
	if err != nil {
		t.Fatal(err)
	}
	if got < n*9/10 || got > n*11/10 {
		t.Fatalf("cardinality %d outside [%d,%d]", got, n*9/10, n*11/10)
	}

	snap, err := eng.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	restored := cardinality.NewEngine(hll.Algorithm{})
	if err := restored.Unmarshal(snap); err != nil {
		t.Fatal(err)
	}
	again, err := restored.Cardinality("g")
	if err != nil {
		t.Fatal(err)
	}
	if again != got {
		t.Fatalf("roundtrip: before=%d after=%d", got, again)
	}
}

// TestEngine_MergeBytes covers the wire-facing merge path added for the
// MERGE_SKETCH handler, including algo-mismatch rejection.
func TestEngine_MergeBytes(t *testing.T) {
	src := cardinality.NewEngine(hll.Algorithm{})
	for i := 0; i < 500; i++ {
		if err := src.Add("g", uint64(i)); err != nil {
			t.Fatal(err)
		}
	}
	payload, err := src.Bytes("g")
	if err != nil {
		t.Fatal(err)
	}

	dst := cardinality.NewEngine(hll.Algorithm{})
	if err := dst.MergeBytes("g", "hll", payload); err != nil {
		t.Fatalf("MergeBytes: %v", err)
	}
	got, err := dst.Cardinality("g")
	if err != nil {
		t.Fatal(err)
	}
	if got == 0 {
		t.Fatal("merge produced empty sketch")
	}

	if err := dst.MergeBytes("g", "bitmap", payload); !errors.Is(err, cardinality.ErrAlgoMismatch) {
		t.Fatalf("want ErrAlgoMismatch, got %v", err)
	}
}
