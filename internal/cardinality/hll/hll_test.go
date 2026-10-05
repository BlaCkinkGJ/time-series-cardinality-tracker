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

package hll

import (
	"bytes"
	"encoding/binary"
	"math/bits"
	"strconv"
	"testing"

	"github.com/spaolacci/murmur3"
	"github.com/BlaCkinkGJ/time-series-cardinality-tracker/internal/cardinality"
)

func TestAlgorithm_Name(t *testing.T) {
	if got := (Algorithm{}).Name(); got != algoName {
		t.Fatalf("Name = %q, want %q", got, algoName)
	}
}

func TestAlgorithm_New(t *testing.T) {
	sk := Algorithm{}.New()
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

func TestEstimate_Empty(t *testing.T) {
	if got := (Algorithm{}).New().Cardinality(); got != 0 {
		t.Fatalf("empty HLL want 0, got %d", got)
	}
}

func TestEstimate_UniqueValues(t *testing.T) {
	sk := Algorithm{}.New()
	const n = 100_000
	for i := 0; i < n; i++ {
		sk.Add(uint64(i))
	}
	est := int64(sk.Cardinality())
	errPct := float64(est-int64(n)) / float64(n) * 100
	if errPct < -3 || errPct > 3 {
		t.Fatalf("estimate %d vs actual %d — error %.2f%% exceeds 3%%", est, n, errPct)
	}
}

func TestMerge(t *testing.T) {
	a, b := Algorithm{}.New(), Algorithm{}.New()
	for i := 0; i < 50_000; i++ {
		a.Add(uint64(i))
	}
	for i := 0; i < 50_000; i++ {
		b.Add(uint64(1_000_000 + i))
	}
	a.Merge(b)
	est := a.Cardinality()
	if est < 90_000 || est > 110_000 {
		t.Fatalf("merged estimate %d outside expected [90k,110k]", est)
	}
}

func TestSketch_MergeNonHLLPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on merging a non-hll sketch")
		}
	}()
	Algorithm{}.New().Merge(bitmapLike{})
}

// bitmapLike is a non-HLL Sketch stub for the type-guard test.
type bitmapLike struct{}

func (bitmapLike) AlgoName() string               { return "bitmap" }
func (bitmapLike) Add(uint64)                     {}
func (bitmapLike) Cardinality() uint64            { return 0 }
func (bitmapLike) Merge(other cardinality.Sketch) {}
func (bitmapLike) Bytes() []byte                  { return nil }
func (bitmapLike) Clone() cardinality.Sketch      { return bitmapLike{} }

func TestBytesParseRoundTrip(t *testing.T) {
	alg := Algorithm{}
	src := alg.New()
	for i := 0; i < 1000; i++ {
		src.Add(uint64(i))
	}
	before := src.Cardinality()

	parsed, err := alg.Parse(src.Bytes())
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if got := parsed.Cardinality(); got != before {
		t.Fatalf("roundtrip: before=%d after=%d", before, got)
	}
	if parsed.AlgoName() != algoName {
		t.Fatalf("AlgoName = %q, want %q", parsed.AlgoName(), algoName)
	}
}

// TestBytes_Layout pins the wire format: 2-byte LE precision header
// followed by exactly numRegs registers.
func TestBytes_Layout(t *testing.T) {
	b := Algorithm{}.New().Bytes()
	if len(b) != 2+numRegs {
		t.Fatalf("len(Bytes()) = %d, want %d", len(b), 2+numRegs)
	}
	if p := binary.LittleEndian.Uint16(b[:2]); p != precision {
		t.Fatalf("precision header = %d, want %d", p, precision)
	}
}

// TestSketch_ParseCorrupt verifies Parse rejects malformed bytes.
func TestSketch_ParseCorrupt(t *testing.T) {
	if _, err := (Algorithm{}).Parse(bytes.Repeat([]byte{0xff}, 4)); err == nil {
		t.Fatal("expected error on short/corrupt bytes, got nil")
	}
	bad := make([]byte, 2+numRegs)
	binary.LittleEndian.PutUint16(bad[:2], precision+1)
	if _, err := (Algorithm{}).Parse(bad); err == nil {
		t.Fatal("expected error on precision mismatch, got nil")
	}
}

// TestSketch_Clone_Deep verifies Clone returns an independent copy.
// Asserted at the register level rather than via Cardinality: an HLL
// estimate for 3 ids can coincide by chance, the bytes cannot.
func TestSketch_Clone_Deep(t *testing.T) {
	alg := Algorithm{}
	a := alg.New()
	a.Add(1)
	a.Add(2)
	a.Add(3)
	before := a.Bytes()

	clone := a.Clone().(*sketch)
	for i := 0; i < 10_000; i++ {
		clone.Add(uint64(1000 + i))
	}
	if !bytes.Equal(a.Bytes(), before) {
		t.Fatal("original register state changed after mutating the clone")
	}
	if bytes.Equal(clone.Bytes(), before) {
		t.Fatal("clone shares register state with the original")
	}
}

// TestAddHashCompat is the issue #13 decision-2 regression guard: Add
// must hash the decimal form of the id, byte-for-byte compatible with
// state persisted by the pre-migration hllAdder path. Changing the hash
// input silently re-hashes existing Badger/raft-snapshot data.
func TestAddHashCompat(t *testing.T) {
	const id = uint64(123)

	sk := Algorithm{}.New().(*sketch)
	sk.Add(id)

	h := murmur3.Sum64([]byte(strconv.FormatUint(id, 10)))
	idx := h >> (64 - precision)
	w := h<<precision | (1<<precision - 1)
	rho := uint8(bits.LeadingZeros64(w)) + 1

	want := make([]byte, 2+numRegs)
	binary.LittleEndian.PutUint16(want[:2], precision)
	want[2+idx] = rho

	if !bytes.Equal(sk.Bytes(), want) {
		t.Fatalf("register state diverged from legacy decimal-string hashing at idx %d", idx)
	}
}
