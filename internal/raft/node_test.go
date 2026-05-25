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

package raft_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/yourorg/cardinality-tracker/internal/hll"
	"github.com/yourorg/cardinality-tracker/internal/raft"
	"github.com/yourorg/cardinality-tracker/internal/store"
)

func TestSingleNodePropose(t *testing.T) {
	dir, err := os.MkdirTemp("", "raft-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	st, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	eng := hll.NewEngine()
	node := raft.NewNode(1, []raft.Peer{{ID: 1}}, eng, st)
	go node.Run()
	defer node.Stop()

	// Wait for leader election (single node: 10 election ticks × 100ms = 1s min)
	time.Sleep(1500 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := node.ProposeAdd(ctx, "ts-x", "hello"); err != nil {
		t.Fatalf("ProposeAdd: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	if est := eng.Estimate("ts-x"); est == 0 {
		t.Fatal("expected non-zero estimate after propose")
	}
}

func TestSnapshotRoundTrip(t *testing.T) {
	eng := hll.NewEngine()
	for i := 0; i < 5000; i++ {
		eng.Add("ts-snap", fmt.Sprintf("v%d", i))
	}
	before := eng.Estimate("ts-snap")

	snap, err := raft.SnapshotEngine(eng)
	if err != nil {
		t.Fatal(err)
	}

	eng2 := hll.NewEngine()
	if err := raft.RestoreEngine(eng2, snap); err != nil {
		t.Fatal(err)
	}
	if eng2.Estimate("ts-snap") != before {
		t.Fatalf("snapshot restore: before=%d after=%d", before, eng2.Estimate("ts-snap"))
	}
}
