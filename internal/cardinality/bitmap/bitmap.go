package bitmap

import (
	"github.com/RoaringBitmap/roaring/roaring64"
)

// Bitmap is a thread-unsafe wrapper around roaring64.Bitmap.
// It represents an exact set of uint64 values with cardinality = popcount.
type Bitmap struct {
	rb *roaring64.Bitmap
}

// New returns an empty bitmap.
func New() *Bitmap {
	return &Bitmap{rb: roaring64.New()}
}

// Add inserts id into the bitmap (no-op if already present).
func (b *Bitmap) Add(id uint64) {
	b.rb.Add(id)
}

// GetCardinality returns the exact number of unique ids.
func (b *Bitmap) GetCardinality() uint64 {
	return b.rb.GetCardinality()
}

// Or merges other into b (b = b ∪ other). Mutates b.
func (b *Bitmap) Or(other *Bitmap) {
	b.rb.Or(other.rb)
}

// Clone returns a deep copy.
func (b *Bitmap) Clone() *Bitmap {
	return &Bitmap{rb: b.rb.Clone()}
}

// Marshal serialises to a portable byte slice (roaring64 native format).
func (b *Bitmap) Marshal() ([]byte, error) {
	return b.rb.MarshalBinary()
}

// Unmarshal parses bytes into b, replacing existing state.
func (b *Bitmap) Unmarshal(data []byte) error {
	r := roaring64.New()
	if err := r.UnmarshalBinary(data); err != nil {
		return err
	}
	b.rb = r
	return nil
}
