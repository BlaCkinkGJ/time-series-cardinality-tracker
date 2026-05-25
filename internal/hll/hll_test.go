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

package hll_test

import (
	"fmt"
	"testing"

	"github.com/yourorg/cardinality-tracker/internal/hll"
)

func TestEstimate_Empty(t *testing.T) {
	h := hll.New()
	if got := h.Estimate(); got != 0 {
		t.Fatalf("empty HLL want 0, got %d", got)
	}
}

func TestEstimate_UniqueValues(t *testing.T) {
	h := hll.New()
	n := 100_000
	for i := 0; i < n; i++ {
		h.Add([]byte(fmt.Sprintf("val-%d", i)))
	}
	est := h.Estimate()
	errPct := float64(int64(est)-int64(n)) / float64(n) * 100
	if errPct < -3 || errPct > 3 {
		t.Fatalf("estimate %d vs actual %d — error %.2f%% exceeds 3%%", est, n, errPct)
	}
}

func TestMerge(t *testing.T) {
	a, b := hll.New(), hll.New()
	for i := 0; i < 50_000; i++ {
		a.Add([]byte(fmt.Sprintf("a-%d", i)))
	}
	for i := 0; i < 50_000; i++ {
		b.Add([]byte(fmt.Sprintf("b-%d", i)))
	}
	a.Merge(b)
	est := a.Estimate()
	if est < 90_000 || est > 110_000 {
		t.Fatalf("merged estimate %d outside expected [90k,110k]", est)
	}
}

func TestMarshalRoundTrip(t *testing.T) {
	h := hll.New()
	for i := 0; i < 1000; i++ {
		h.Add([]byte(fmt.Sprintf("v%d", i)))
	}
	before := h.Estimate()
	b, err := h.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	h2, err := hll.Unmarshal(b)
	if err != nil {
		t.Fatal(err)
	}
	if h2.Estimate() != before {
		t.Fatalf("marshal roundtrip: before=%d after=%d", before, h2.Estimate())
	}
}
