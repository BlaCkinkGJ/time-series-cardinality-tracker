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

package server

import (
	"context"
	"testing"

	pb "github.com/BlaCkinkGJ/time-series-cardinality-tracker/gen/cardinality/v1"
	"github.com/BlaCkinkGJ/time-series-cardinality-tracker/internal/cardinality"
	"github.com/BlaCkinkGJ/time-series-cardinality-tracker/internal/cardinality/hll"
	"github.com/BlaCkinkGJ/time-series-cardinality-tracker/internal/store"
)

// TestMergeStandalone exercises Merge without a Raft log: the union lands in
// the engine, is persisted for the next restart, and bad input fails closed.
func TestMergeStandalone(t *testing.T) {
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })

	eng := cardinality.NewEngine(hll.Algorithm{})
	srv := New(eng, st, nil, nil, "") // nil raft → standalone
	ctx := context.Background()

	// A sketch built elsewhere, for ids 0..99.
	remote := hll.Algorithm{}.New()
	for i := 0; i < 100; i++ {
		remote.Add(uint64(i))
	}
	req := &pb.MergeRequest{Group: "g", Algo: remote.AlgoName(), Sketch: remote.Bytes()}

	if _, err := srv.Merge(ctx, req); err != nil {
		t.Fatalf("Merge: %v", err)
	}
	if card, err := eng.Cardinality("g"); err != nil || card != 100 {
		t.Fatalf("merged cardinality = %d (err=%v), want 100", card, err)
	}

	// A restart reads only the store, so the union must be there.
	raw, err := st.Load("g")
	if err != nil {
		t.Fatalf("merged group was not persisted: %v", err)
	}
	fresh := cardinality.NewEngine(hll.Algorithm{})
	if err := fresh.Restore("g", raw); err != nil {
		t.Fatal(err)
	}
	if restored, err := fresh.Cardinality("g"); err != nil || restored != 100 {
		t.Fatalf("restored cardinality = %d (err=%v), want 100", restored, err)
	}

	// Merging the same sketch again unions it with itself: no double count.
	if _, err := srv.Merge(ctx, req); err != nil {
		t.Fatalf("re-merge: %v", err)
	}
	if card, _ := eng.Cardinality("g"); card != 100 {
		t.Fatalf("re-merge changed cardinality to %d, want 100", card)
	}

	rejected := []struct {
		name string
		req  *pb.MergeRequest
	}{
		{"algo mismatch", &pb.MergeRequest{Group: "g", Algo: "bitmap", Sketch: remote.Bytes()}},
		{"empty sketch", &pb.MergeRequest{Group: "g", Algo: remote.AlgoName()}},
		{"empty group", &pb.MergeRequest{Algo: remote.AlgoName(), Sketch: remote.Bytes()}},
	}
	for _, c := range rejected {
		if _, err := srv.Merge(ctx, c.req); err == nil {
			t.Errorf("%s: expected an error", c.name)
		}
	}
	if card, _ := eng.Cardinality("g"); card != 100 {
		t.Fatalf("a rejected merge mutated the group: cardinality %d", card)
	}
}
