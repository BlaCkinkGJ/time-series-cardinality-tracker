package cardinality

// Sketch is the algorithm-agnostic cardinality sketch contract.
//
// Implementations are not required to be safe for concurrent use;
// the per-group Engine serializes access.
type Sketch interface {
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

// Algorithm is a factory for a particular sketch kind.
// Implementations register themselves via Register.
type Algorithm interface {
	// Name returns the registry key (e.g. "bitmap", "bitset", "hll").
	Name() string

	// New returns an empty sketch of this kind.
	New() Sketch

	// Parse deserialises bytes produced by Sketch.Bytes into a sketch.
	Parse(b []byte) (Sketch, error)
}

// registry holds the package-global algorithm table.
// It is process-wide and only modified at init or test time.
var registry = map[string]Algorithm{}

// Register adds an algorithm to the registry. If an algorithm with
// the same name is already registered, the new one replaces it (last-wins).
// Register is intended to be called from package init() functions.
func Register(a Algorithm) {
	registry[a.Name()] = a
}

// Get returns the algorithm registered under name, if any.
func Get(name string) (Algorithm, bool) {
	a, ok := registry[name]
	return a, ok
}

// resetRegistry clears the registry. For tests only.
func resetRegistry() {
	registry = map[string]Algorithm{}
}
