package bitmap

import (
	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/yourorg/cardinality-tracker/internal/cardinality"
)

// algoName is the algo key for this algorithm.
const algoName = "bitmap"

// Algorithm is the roaring64-backed cardinality factory. Pass an
// instance to cardinality.NewEngine to enable bitmap sketches.
type Algorithm struct{}

// Name returns the algo key.
func (Algorithm) Name() string { return algoName }

// New returns an empty roaring64-backed sketch.
func (Algorithm) New() cardinality.Sketch { return newSketch(roaring64.New()) }

// Parse decodes roaring64-native bytes into a sketch.
func (Algorithm) Parse(b []byte) (cardinality.Sketch, error) {
	rb := roaring64.New()
	if err := rb.UnmarshalBinary(b); err != nil {
		return nil, err
	}
	return newSketch(rb), nil
}

// sketch is a roaring64-backed cardinality.Sketch. The Engine
// enforces same-algo per group, so a non-bitmap sketch here
// indicates a programming error.
type sketch struct{ rb *roaring64.Bitmap }

func newSketch(rb *roaring64.Bitmap) *sketch { return &sketch{rb: rb} }

func (s *sketch) AlgoName() string          { return algoName }
func (s *sketch) Add(id uint64)             { s.rb.Add(id) }
func (s *sketch) Cardinality() uint64       { return s.rb.GetCardinality() }
func (s *sketch) Bytes() []byte             { b, _ := s.rb.MarshalBinary(); return b }
func (s *sketch) Clone() cardinality.Sketch { return newSketch(s.rb.Clone()) }
func (s *sketch) Merge(other cardinality.Sketch) {
	o, ok := other.(*sketch)
	if !ok {
		panic("bitmap: merge with non-bitmap sketch")
	}
	s.rb.Or(o.rb)
}
