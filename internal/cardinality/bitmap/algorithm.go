package bitmap

import "github.com/yourorg/cardinality-tracker/internal/cardinality"

// algoName is the algo key for this algorithm.
const algoName = "bitmap"

// Algorithm is the bitmap-backed cardinality factory. Pass an
// instance to cardinality.NewEngine to enable bitmap sketches.
//
// It is the adapter between the algorithm-agnostic cardinality.Sketch
// interface and the concrete *Bitmap type introduced in PR #7. The
// Bitmap itself is left unchanged; this file adds only the bridge.
type Algorithm struct{}

// Name returns the algo key.
func (Algorithm) Name() string { return algoName }

// New returns an empty bitmap-backed sketch.
func (Algorithm) New() cardinality.Sketch { return newSketch(New()) }

// Parse decodes bytes produced by (*Bitmap).Marshal into a sketch.
func (Algorithm) Parse(b []byte) (cardinality.Sketch, error) {
	sk := New()
	if err := sk.Unmarshal(b); err != nil {
		return nil, err
	}
	return newSketch(sk), nil
}

// sketch adapts *Bitmap to cardinality.Sketch.
//
// It wraps rather than embeds because *Bitmap is a concrete type we
// must not modify; the wrapper delegates Add, Bytes, and Clone, and
// bridges GetCardinality -> Cardinality and the concrete Or method
// to the interface-typed Merge.
type sketch struct{ b *Bitmap }

func newSketch(b *Bitmap) *sketch { return &sketch{b: b} }

func (s *sketch) AlgoName() string          { return algoName }
func (s *sketch) Add(id uint64)             { s.b.Add(id) }
func (s *sketch) Cardinality() uint64       { return s.b.GetCardinality() }
func (s *sketch) Bytes() []byte             { b, _ := s.b.Marshal(); return b }
func (s *sketch) Clone() cardinality.Sketch { return newSketch(s.b.Clone()) }

// Merge unions other into s in place. other must be a bitmap sketch;
// the Engine enforces same-algo per group, so a type mismatch here
// indicates a programming error.
func (s *sketch) Merge(other cardinality.Sketch) {
	o, ok := other.(*sketch)
	if !ok {
		panic("bitmap: merge with non-bitmap sketch")
	}
	s.b.Or(o.b)
}
