package cardinality

// Sketch is the algorithm-agnostic cardinality sketch contract.
//
// Each sketch self-identifies its algorithm via AlgoName so callers
// (e.g. Engine.Merge) can match sketches against their owning group
// without reflection.
//
// Implementations are not required to be safe for concurrent use;
// the per-group Engine serializes access.
type Sketch interface {
	// AlgoName returns the registry key of the algorithm that produced
	// this sketch (e.g. "bitmap", "hll"). It must equal Algorithm.Name().
	AlgoName() string

	// Add inserts id into the sketch (no-op if already tracked).
	Add(id uint64)

	// Cardinality returns the exact or estimated unique count.
	Cardinality() uint64

	// Merge unions other into the receiver in place.
	Merge(other Sketch)

	// Bytes serialises the sketch to a portable byte slice.
	Bytes() []byte

	// Clone returns an independent deep copy.
	Clone() Sketch
}

// Algorithm is a factory for a particular sketch kind. Pass instances
// to NewEngine to register them.
type Algorithm interface {
	// Name returns the algo key (must match Sketch.AlgoName).
	Name() string

	// New returns an empty sketch of this kind.
	New() Sketch

	// Parse deserialises bytes produced by Sketch.Bytes into a sketch.
	Parse(b []byte) (Sketch, error)
}
