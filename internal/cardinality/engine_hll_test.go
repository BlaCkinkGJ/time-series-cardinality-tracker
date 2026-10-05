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

package cardinality_test

import (
	"errors"
	"testing"

	"github.com/BlaCkinkGJ/time-series-cardinality-tracker/internal/cardinality"
	"github.com/BlaCkinkGJ/time-series-cardinality-tracker/internal/cardinality/hll"
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
	var payload []byte
	if err := src.Persist("g", func(b []byte) error {
		payload = append([]byte(nil), b...)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := src.Persist("missing", func([]byte) error { return nil }); !errors.Is(err, cardinality.ErrUnknownGroup) {
		t.Fatalf("want ErrUnknownGroup for missing group, got %v", err)
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
