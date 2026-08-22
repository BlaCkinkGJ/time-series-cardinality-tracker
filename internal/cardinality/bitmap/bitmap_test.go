package bitmap

import (
	"bytes"
	"testing"
)

func TestAdd_Unique(t *testing.T) {
	b := New()
	for i := uint64(0); i < 1000; i++ {
		b.Add(i)
	}
	if got := b.GetCardinality(); got != 1000 {
		t.Fatalf("cardinality = %d, want 1000", got)
	}
}

func TestAdd_Duplicate(t *testing.T) {
	b := New()
	for i := 0; i < 1000; i++ {
		b.Add(42)
	}
	if got := b.GetCardinality(); got != 1 {
		t.Fatalf("cardinality = %d, want 1", got)
	}
}

func TestAdd_Mixed(t *testing.T) {
	b := New()
	for i := uint64(0); i < 1000; i++ {
		b.Add(i)
	}
	for i := 0; i < 5000; i++ {
		b.Add(uint64(i % 1000))
	}
	if got := b.GetCardinality(); got != 1000 {
		t.Fatalf("cardinality = %d, want 1000", got)
	}
}

func TestGetCardinality_Empty(t *testing.T) {
	b := New()
	if got := b.GetCardinality(); got != 0 {
		t.Fatalf("cardinality = %d, want 0", got)
	}
}

func TestOr_Disjoint(t *testing.T) {
	a := New()
	a.Add(1)
	a.Add(2)
	a.Add(3)
	c := New()
	c.Add(4)
	c.Add(5)
	c.Add(6)
	a.Or(c)
	if got := a.GetCardinality(); got != 6 {
		t.Fatalf("cardinality = %d, want 6", got)
	}
}

func TestOr_Overlap(t *testing.T) {
	a := New()
	a.Add(1)
	a.Add(2)
	a.Add(3)
	c := New()
	c.Add(3)
	c.Add(4)
	c.Add(5)
	a.Or(c)
	if got := a.GetCardinality(); got != 5 {
		t.Fatalf("cardinality = %d, want 5", got)
	}
}

func TestOr_Self(t *testing.T) {
	a := New()
	a.Add(1)
	a.Add(2)
	a.Add(3)
	a.Or(a)
	if got := a.GetCardinality(); got != 3 {
		t.Fatalf("cardinality = %d, want 3", got)
	}
}

func TestOr_Empty(t *testing.T) {
	a := New()
	a.Add(1)
	a.Add(2)
	a.Add(3)
	empty := New()
	a.Or(empty)
	if got := a.GetCardinality(); got != 3 {
		t.Fatalf("cardinality = %d, want 3", got)
	}
}

func TestOr_Commutative(t *testing.T) {
	a := New()
	a.Add(1)
	a.Add(2)
	a.Add(3)
	c := New()
	c.Add(3)
	c.Add(4)
	c.Add(5)

	ab := a.Clone()
	ab.Or(c)

	ba := c.Clone()
	ba.Or(a)

	if ab.GetCardinality() != ba.GetCardinality() {
		t.Fatalf("not commutative: a|b=%d b|a=%d", ab.GetCardinality(), ba.GetCardinality())
	}
}

func TestOr_Associative(t *testing.T) {
	a := New()
	a.Add(1)
	a.Add(2)
	c := New()
	c.Add(2)
	c.Add(3)
	d := New()
	d.Add(3)
	d.Add(4)

	left := a.Clone()
	left.Or(c)
	left.Or(d)

	right := c.Clone()
	right.Or(d)
	ra := a.Clone()
	ra.Or(right)

	if left.GetCardinality() != ra.GetCardinality() {
		t.Fatalf("not associative: (a|c)|d=%d a|(c|d)=%d", left.GetCardinality(), ra.GetCardinality())
	}
}

func TestClone_Deep(t *testing.T) {
	a := New()
	a.Add(1)
	a.Add(2)
	a.Add(3)
	c := a.Clone()
	c.Add(4)
	if a.GetCardinality() != 3 {
		t.Fatalf("original changed after clone mutation: %d", a.GetCardinality())
	}
	if c.GetCardinality() != 4 {
		t.Fatalf("clone cardinality = %d, want 4", c.GetCardinality())
	}
}

func TestMarshal_Roundtrip(t *testing.T) {
	a := New()
	for i := uint64(0); i < 100; i++ {
		a.Add(i * 7)
	}
	data, err := a.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	b := New()
	if err := b.Unmarshal(data); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if a.GetCardinality() != b.GetCardinality() {
		t.Fatalf("cardinality mismatch: a=%d b=%d", a.GetCardinality(), b.GetCardinality())
	}
}

func TestMarshal_Empty(t *testing.T) {
	a := New()
	data, err := a.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	b := New()
	if err := b.Unmarshal(data); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if b.GetCardinality() != 0 {
		t.Fatalf("cardinality = %d, want 0", b.GetCardinality())
	}
}

func TestUnmarshal_Corrupt(t *testing.T) {
	b := New()
	// ponytail: assert-style check. invalid bytes should return error, not panic.
	if err := b.Unmarshal(bytes.Repeat([]byte{0xff}, 4)); err == nil {
		t.Fatalf("unmarshal of corrupt bytes returned nil error")
	}
}
