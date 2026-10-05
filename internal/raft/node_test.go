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
	"os"
	"testing"
	"time"

	"github.com/BlaCkinkGJ/time-series-cardinality-tracker/internal/cardinality"
	"github.com/BlaCkinkGJ/time-series-cardinality-tracker/internal/cardinality/hll"
	"github.com/BlaCkinkGJ/time-series-cardinality-tracker/internal/raft"
	"github.com/BlaCkinkGJ/time-series-cardinality-tracker/internal/store"
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

	eng := cardinality.NewEngine(hll.Algorithm{})
	node := raft.NewNode(1, []raft.Peer{{ID: 1}}, eng, st)
	go node.Run()
	defer node.Stop()

	// Wait for leader election (single node: 10 election ticks × 100ms = 1s min)
	time.Sleep(1500 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := node.ProposeAdd(ctx, "ts-x", 42); err != nil {
		t.Fatalf("ProposeAdd: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	card, err := eng.Cardinality("ts-x")
	if err != nil {
		t.Fatalf("Cardinality: %v", err)
	}
	if card == 0 {
		t.Fatal("expected non-zero cardinality after propose")
	}

	// The apply path must also persist the sketch to Badger.
	deadline := time.Now().Add(2 * time.Second)
	for {
		raw, err := st.Load("ts-x")
		if err == nil && len(raw) > 0 {
			sk, perr := (hll.Algorithm{}).Parse(raw)
			if perr != nil {
				t.Fatalf("parse persisted sketch: %v", perr)
			}
			if sk.Cardinality() == 0 {
				t.Fatal("persisted sketch is empty")
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("apply did not persist the sketch to the store: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestSnapshotRoundTrip(t *testing.T) {
	eng := cardinality.NewEngine(hll.Algorithm{})
	for i := 0; i < 5000; i++ {
		if err := eng.Add("ts-snap", uint64(i)); err != nil {
			t.Fatal(err)
		}
	}
	before, err := eng.Cardinality("ts-snap")
	if err != nil {
		t.Fatal(err)
	}

	snap, err := eng.Marshal()
	if err != nil {
		t.Fatal(err)
	}

	eng2 := cardinality.NewEngine(hll.Algorithm{})
	if err := eng2.Unmarshal(snap); err != nil {
		t.Fatal(err)
	}
	after, err := eng2.Cardinality("ts-snap")
	if err != nil {
		t.Fatal(err)
	}
	if after != before {
		t.Fatalf("snapshot restore: before=%d after=%d", before, after)
	}
}
