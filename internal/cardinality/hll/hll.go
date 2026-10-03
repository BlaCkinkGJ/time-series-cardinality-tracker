// Package hll implements HyperLogLog++ (precision 14) as a
// cardinality.Algorithm. Pass hll.Algorithm{} to cardinality.NewEngine.
package hll

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/bits"
	"strconv"

	"github.com/spaolacci/murmur3"
	"github.com/yourorg/cardinality-tracker/internal/cardinality"
)

// algoName is the wire/registry key for HLL sketches: the value carried in
// MERGE_SKETCH payloads and reported by Algorithm.Name/Sketch.AlgoName.
const algoName = "hll"

const (
	precision = 14
	numRegs   = 1 << precision // 16384
)

// alphaMM is computed at runtime to avoid float const precision issues.
var alphaMM = (0.7213 / (1.0 + 1.079/float64(numRegs))) * numRegs * numRegs

// Algorithm is the HyperLogLog++ factory.
type Algorithm struct{}

// Name returns the algo key.
func (Algorithm) Name() string { return algoName }

// New returns an empty HLL sketch.
func (Algorithm) New() cardinality.Sketch { return &sketch{} }

// Parse decodes the wire format produced by sketch.Bytes:
// [uint16 precision LE][numRegs registers].
func (Algorithm) Parse(b []byte) (cardinality.Sketch, error) {
	if len(b) < 2+numRegs {
		return nil, errors.New("hll: buffer too short")
	}
	if p := binary.LittleEndian.Uint16(b[:2]); p != precision {
		return nil, fmt.Errorf("hll: precision mismatch: got %d want %d", p, precision)
	}
	s := &sketch{}
	copy(s.regs[:], b[2:])
	return s, nil
}

// sketch is a cardinality.Sketch backed by an HLL register array.
type sketch struct {
	regs [numRegs]uint8
}

// AlgoName returns the algo key.
func (s *sketch) AlgoName() string { return algoName }

// Add inserts id. The id is hashed in its decimal form to stay
// byte-compatible with pre-migration state: sketches persisted in
// Badger and raft snapshots were produced by
// murmur3.Sum64([]byte(strconv.FormatUint(id, 10))). Hashing the raw
// 8 bytes instead would silently change existing cardinalities.
func (s *sketch) Add(id uint64) {
	hash := murmur3.Sum64([]byte(strconv.FormatUint(id, 10)))
	idx := hash >> (64 - precision)
	w := hash<<precision | (1<<precision - 1)
	rho := uint8(bits.LeadingZeros64(w)) + 1
	if rho > s.regs[idx] {
		s.regs[idx] = rho
	}
}

// Cardinality returns the estimated unique count.
func (s *sketch) Cardinality() uint64 {
	var sum float64
	var zeros int
	for _, v := range &s.regs {
		sum += math.Pow(2, -float64(v))
		if v == 0 {
			zeros++
		}
	}
	est := alphaMM / sum
	// Small range correction (linear counting).
	if est <= 2.5*numRegs && zeros > 0 {
		est = numRegs * math.Log(float64(numRegs)/float64(zeros))
	}
	return uint64(math.Round(est))
}

// Merge unions other into the receiver by element-wise max. A non-HLL
// sketch indicates a programming error (the Engine enforces one
// algorithm).
func (s *sketch) Merge(other cardinality.Sketch) {
	o, ok := other.(*sketch)
	if !ok {
		panic("hll: merge with non-hll sketch")
	}
	for i := range s.regs {
		if o.regs[i] > s.regs[i] {
			s.regs[i] = o.regs[i]
		}
	}
}

// Bytes serialises the sketch.
func (s *sketch) Bytes() []byte {
	b := make([]byte, 2+numRegs)
	binary.LittleEndian.PutUint16(b[:2], precision)
	copy(b[2:], s.regs[:])
	return b
}

// Clone returns an independent deep copy.
func (s *sketch) Clone() cardinality.Sketch {
	c := &sketch{}
	copy(c.regs[:], s.regs[:])
	return c
}
