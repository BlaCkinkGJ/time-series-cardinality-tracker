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

// TypeBatchAdd is the registered command type for the BATCH_ADD handler.
// Payload schema:
//
//	[varint n] [varint id_1] [varint id_2] ... [varint id_n]
//
// n == 0 is a no-op success (no Adder calls).
const TypeBatchAdd = "BATCH_ADD"

func init() {
	RegisterHandler(TypeBatchAdd, applyBatchAdd)
}

func applyBatchAdd(cmd *pb.Command, apply Adder) error {
	buf := cmd.Payload
	n, k := binary.Uvarint(buf)
	if k <= 0 {
		return fmt.Errorf("%w: missing count varint", ErrBadPayload)
	}
	buf = buf[k:]
	for i := uint64(0); i < n; i++ {
		id, m := binary.Uvarint(buf)
		if m <= 0 {
			return fmt.Errorf("%w: id %d/%d truncated", ErrBadPayload, i+1, n)
		}
		if err := apply.Add(cmd.Group, id); err != nil {
			return err
		}
		buf = buf[m:]
	}
	return nil
}