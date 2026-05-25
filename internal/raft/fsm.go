// internal/raft/fsm.go
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
