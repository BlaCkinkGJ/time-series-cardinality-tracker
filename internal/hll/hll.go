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
	"encoding/binary"
	"errors"
	"math"
	"math/bits"

	"github.com/spaolacci/murmur3"
)

const (
	precision = 14
	numRegs   = 1 << precision // 16384
	regMask   = numRegs - 1
)

// alphaMM is computed at runtime to avoid float const precision issues.
var alphaMM = (0.7213 / (1.0 + 1.079/float64(numRegs))) * numRegs * numRegs

// HLL holds HyperLogLog++ state for a single time-series.
type HLL struct {
	regs [numRegs]uint8
}

// New returns an empty HLL.
func New() *HLL { return &HLL{} }

// Add inserts a value into the HLL.
func (h *HLL) Add(value []byte) {
	hash := murmur3.Sum64(value)
	idx := hash >> (64 - precision)
	w := hash<<precision | (1<<precision - 1)
	rho := uint8(bits.LeadingZeros64(w)) + 1
	if rho > h.regs[idx] {
		h.regs[idx] = rho
	}
}

// Estimate returns the estimated cardinality.
func (h *HLL) Estimate() uint64 {
	var sum float64
	var zeros int
	for _, v := range &h.regs {
		sum += math.Pow(2, -float64(v))
		if v == 0 {
			zeros++
		}
	}
	est := alphaMM / sum
	// Small range correction (linear counting)
	if est <= 2.5*numRegs && zeros > 0 {
		est = numRegs * math.Log(float64(numRegs)/float64(zeros))
	}
	return uint64(math.Round(est))
}

// Merge combines two HLLs by taking element-wise max.
func (h *HLL) Merge(other *HLL) {
	for i := range h.regs {
		if other.regs[i] > h.regs[i] {
			h.regs[i] = other.regs[i]
		}
	}
}

// Marshal serialises the HLL to bytes.
func (h *HLL) Marshal() ([]byte, error) {
	buf := make([]byte, 2+numRegs)
	binary.LittleEndian.PutUint16(buf[:2], precision)
	copy(buf[2:], h.regs[:])
	return buf, nil
}

// Unmarshal deserialises bytes into a new HLL.
func Unmarshal(b []byte) (*HLL, error) {
	if len(b) < 2+numRegs {
		return nil, errors.New("hll: buffer too short")
	}
	p := binary.LittleEndian.Uint16(b[:2])
	if p != precision {
		return nil, errors.New("hll: precision mismatch")
	}
	h := &HLL{}
	copy(h.regs[:], b[2:])
	return h, nil
}
