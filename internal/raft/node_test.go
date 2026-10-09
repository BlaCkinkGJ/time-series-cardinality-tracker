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

// TestProposeBatch_OneEntry proves a batch is a single Raft entry: two 1000-id
// batches land on consecutive log indices. (The applied *index* cannot be
// compared against a fresh node's zero, because the initial empty entry and
// the election no-op consume indices without being applied.)
func TestProposeBatch_OneEntry(t *testing.T) {
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	eng := cardinality.NewEngine(hll.Algorithm{})
	node := raft.NewNode(1, []raft.Peer{{ID: 1}}, eng, st)
	go node.Run()
	defer node.Stop()

	time.Sleep(1500 * time.Millisecond) // leader election

	ids := make([]uint64, 1000)
	for i := range ids {
		ids[i] = uint64(i)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := node.ProposeBatch(ctx, "ts-batch-a", ids); err != nil {
		t.Fatalf("ProposeBatch: %v", err)
	}
	first := waitAppliedAfter(t, node, 0)

	if err := node.ProposeBatch(ctx, "ts-batch-b", ids); err != nil {
		t.Fatalf("ProposeBatch: %v", err)
	}
	second := waitAppliedAfter(t, node, first)

	if second != first+1 {
		t.Fatalf("1000-id batches landed at indices %d and %d, want consecutive", first, second)
	}

	for _, group := range []string{"ts-batch-a", "ts-batch-b"} {
		card, err := eng.Cardinality(group)
		if err != nil {
			t.Fatal(err)
		}
		if card < 970 || card > 1030 {
			t.Fatalf("batch cardinality of %q is %d, want ~1000", group, card)
		}
		// The single entry must persist the whole group once.
		if b, err := st.Load(group); err != nil || len(b) == 0 {
			t.Fatalf("batch entry did not persist %q: %v", group, err)
		}
	}
}

// waitAppliedAfter polls until the FSM applied an entry past index, and
// returns that index.
func waitAppliedAfter(t *testing.T, node *raft.Node, after uint64) uint64 {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, _, applied := node.Status(); applied > after {
			return applied
		}
		if time.Now().After(deadline) {
			t.Fatalf("applied index never moved past %d", after)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestProposeMerge unions an externally built sketch into a group and keeps
// it: the merged count lands in the engine and in the store.
func TestProposeMerge(t *testing.T) {
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	eng := cardinality.NewEngine(hll.Algorithm{})
	node := raft.NewNode(1, []raft.Peer{{ID: 1}}, eng, st)
	go node.Run()
	defer node.Stop()

	time.Sleep(1500 * time.Millisecond) // leader election

	// A sketch produced by another cluster for ids 0..99.
	remote := hll.Algorithm{}.New()
	for i := 0; i < 100; i++ {
		remote.Add(uint64(i))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := node.ProposeMerge(ctx, "ts-merge", remote.AlgoName(), remote.Bytes()); err != nil {
		t.Fatalf("ProposeMerge: %v", err)
	}

	waitCardinality(t, eng, "ts-merge", 100)

	// A later local id unions into the merged sketch instead of replacing it.
	if err := node.ProposeAdd(ctx, "ts-merge", 5000); err != nil {
		t.Fatalf("ProposeAdd: %v", err)
	}
	waitCardinality(t, eng, "ts-merge", 101)
}

// waitCardinality polls the engine until group reports exactly want.
func waitCardinality(t *testing.T, eng *cardinality.Engine, group string, want uint64) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	var last uint64
	for {
		card, err := eng.Cardinality(group)
		if err == nil {
			last = card
			if card == want {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("cardinality of %q is %d, want %d", group, last, want)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
