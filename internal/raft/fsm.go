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

package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"

	"github.com/yourorg/cardinality-tracker/internal/hll"
)

// snapshotData is the serialised form of the full HLL Engine state.
type snapshotData struct {
	Series map[string][]byte // series_id → hll.Marshal() bytes
}

// SnapshotEngine serialises all HLL state to a byte slice.
func SnapshotEngine(eng *hll.Engine) ([]byte, error) {
	sd := snapshotData{Series: make(map[string][]byte)}
	eng.Range(func(id string, h *hll.HLL) {
		b, _ := h.Marshal()
		sd.Series[id] = b
	})
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(sd); err != nil {
		return nil, fmt.Errorf("snapshot encode: %w", err)
	}
	return buf.Bytes(), nil
}

// RestoreEngine deserialises snapshot bytes into eng, replacing all existing state.
func RestoreEngine(eng *hll.Engine, data []byte) error {
	var sd snapshotData
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&sd); err != nil {
		return fmt.Errorf("snapshot decode: %w", err)
	}
	for id, b := range sd.Series {
		h, err := hll.Unmarshal(b)
		if err != nil {
			return err
		}
		eng.Set(id, h)
	}
	return nil
}
