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
	"encoding/binary"
	"fmt"

	pb "github.com/yourorg/cardinality-tracker/gen/cardinality/v1"
)

// TypeMergeSketch is the registered command type for the MERGE_SKETCH
// handler. Payload schema:
//
//	[varint algo_len] [algo_bytes:algo_len] [sketch_payload:rest]
//
// The handler decodes the prefix and forwards (algoName, sketchBytes)
// to Adder.Merge, which owns algorithm-specific parsing and seeding
// or merging of the local group sketch. Per-group algorithm override
// and a true cardinality.Algorithm registry land in #13; today only
// "hll" produces a non-ErrUnknownAlgorithm result.
const TypeMergeSketch = "MERGE_SKETCH"

func init() {
	RegisterHandler(TypeMergeSketch, applyMergeSketch)
}

func applyMergeSketch(cmd *pb.Command, apply Adder) error {
	buf := cmd.Payload
	algoLen, n := binary.Uvarint(buf)
	if n <= 0 {
		return fmt.Errorf("%w: missing algo_len varint", ErrBadPayload)
	}
	buf = buf[n:]
	if uint64(len(buf)) < algoLen {
		return fmt.Errorf("%w: algo_len %d exceeds payload %d", ErrBadPayload, algoLen, len(buf))
	}
	algoName := string(buf[:algoLen])
	sketch := buf[algoLen:]
	return apply.Merge(cmd.Group, algoName, sketch)
}