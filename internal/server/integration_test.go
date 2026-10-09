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

//go:build integration

package server_test

import (
	"context"
	"math"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/BlaCkinkGJ/time-series-cardinality-tracker/gen/cardinality/v1"
	"github.com/BlaCkinkGJ/time-series-cardinality-tracker/internal/cardinality"
	"github.com/BlaCkinkGJ/time-series-cardinality-tracker/internal/cardinality/hll"
	"github.com/BlaCkinkGJ/time-series-cardinality-tracker/internal/raft"
	"github.com/BlaCkinkGJ/time-series-cardinality-tracker/internal/server"
	"github.com/BlaCkinkGJ/time-series-cardinality-tracker/internal/store"
)

// startRaftCluster wires a single-node Raft group behind a gRPC server the
// way cmd/server does, and returns the handles the assertions need. Every
// resource is torn down with the test.
func startRaftCluster(t *testing.T) (pb.CardinalityServiceClient, *cardinality.Engine, *store.BadgerStore, *raft.Node) {
	t.Helper()

	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })

	eng := cardinality.NewEngine(hll.Algorithm{})
	node := raft.NewNode(1, []raft.Peer{{ID: 1}}, eng, st)
	go node.Run()
	t.Cleanup(node.Stop)

	time.Sleep(600 * time.Millisecond) // leader election

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lis.Close() })

	gs := grpc.NewServer()
	pb.RegisterCardinalityServiceServer(gs, server.New(eng, st, node, nil, ""))
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)

	conn, err := grpc.Dial(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	return pb.NewCardinalityServiceClient(conn), eng, st, node
}

func TestIntegration_AddQuery_WithRaft(t *testing.T) {
	client, _, _, _ := startRaftCluster(t)
	ctx := context.Background()

	n := 50000
	for i := 0; i < n; i++ {
		_, err := client.Add(ctx, &pb.AddRequest{Group: "prod-group", Id: uint64(i)})
		if err != nil {
			t.Fatalf("Add %d: %v", i, err)
		}
	}

	resp, err := client.Query(ctx, &pb.QueryRequest{Group: "prod-group", StaleOk: true})
	if err != nil {
		t.Fatal(err)
	}

	errPct := float64(int64(resp.Cardinality)-int64(n)) / float64(n) * 100
	if errPct < -3 || errPct > 3 {
		t.Fatalf("cardinality %d vs %d — %.2f%% error", resp.Cardinality, n, errPct)
	}
	t.Logf("cardinality=%d (expected≈%d, error=%.2f%%)", resp.Cardinality, n, errPct)
}

// TestIntegration_BatchAndMerge_WithRaft drives the two paths #22 wired up
// through the real API: one BATCH_ADD entry for a whole batch, and a
// MERGE_SKETCH entry that unions another cluster's sketch into a group.
func TestIntegration_BatchAndMerge_WithRaft(t *testing.T) {
	const batchGroup = "batch-group"

	client, eng, st, node := startRaftCluster(t)
	ctx := context.Background()

	ids := make([]uint64, 1000)
	for i := range ids {
		ids[i] = uint64(i)
	}

	if _, err := client.BatchAdd(ctx, &pb.BatchAddRequest{Group: batchGroup, Ids: ids}); err != nil {
		t.Fatalf("BatchAdd: %v", err)
	}
	waitPersisted(t, st, "batch-group", len(ids))

	// One entry for the whole batch: the next proposal takes the next index.
	first := waitAppliedAfter(t, node, 0)
	if _, err := client.BatchAdd(ctx, &pb.BatchAddRequest{Group: "batch-group-2", Ids: ids}); err != nil {
		t.Fatalf("BatchAdd: %v", err)
	}
	if second := waitAppliedAfter(t, node, first); second != first+1 {
		t.Fatalf("1000-id batches landed at %d and %d, want consecutive indices", first, second)
	}

	if card, err := eng.Cardinality(batchGroup); err != nil || errPct(card, len(ids)) > 3 {
		t.Fatalf("batch cardinality %d (err=%v), want ~%d", card, err, len(ids))
	}

	// Merge a sketch built for another cluster into a fresh group.
	remote := hll.Algorithm{}.New()
	for i := 0; i < 100; i++ {
		remote.Add(uint64(i))
	}
	if _, err := client.Merge(ctx, &pb.MergeRequest{
		Group:  "merge-group",
		Algo:   remote.AlgoName(),
		Sketch: remote.Bytes(),
	}); err != nil {
		t.Fatalf("Merge: %v", err)
	}
	waitPersisted(t, st, "merge-group", 100)

	if card, err := eng.Cardinality("merge-group"); err != nil || card != 100 {
		t.Fatalf("merged cardinality %d (err=%v), want 100", card, err)
	}

	// A mismatched algorithm must fail at the API, not on apply: Raft accepts
	// a proposed entry whether or not applying it later succeeds, so an
	// unchecked payload would come back as ok:true for a merge that never
	// happened (only the replica's log would know).
	if _, err := client.Merge(ctx, &pb.MergeRequest{
		Group:  "merge-group",
		Algo:   "bitmap",
		Sketch: remote.Bytes(),
	}); err == nil {
		t.Fatal("merge with a mismatched algorithm returned success")
	}
	if card, _ := eng.Cardinality("merge-group"); card != 100 {
		t.Fatalf("rejected merge changed cardinality to %d, want 100", card)
	}
}

// waitAppliedAfter polls raft.Node.Status until the FSM applied an entry past
// index, and returns that index.
func waitAppliedAfter(t *testing.T, node *raft.Node, after uint64) uint64 {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)
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

// TestIntegration_RestartRecovery proves state survives a process restart.
// The Raft log is in-memory, so the only way back is store.LoadAll →
// Engine.Restore — the same sequence cmd/server runs at startup.
func TestIntegration_RestartRecovery(t *testing.T) {
	dir := t.TempDir()
	const (
		group = "restart-group"
		n     = 20000
	)

	// First process: propose n adds, then shut everything down.
	func() {
		st, err := store.Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		defer st.Close()

		eng := cardinality.NewEngine(hll.Algorithm{})
		node := raft.NewNode(1, []raft.Peer{{ID: 1}}, eng, st)
		go node.Run()
		defer node.Stop()

		time.Sleep(600 * time.Millisecond) // leader election

		ctx := context.Background()
		for i := 0; i < n; i++ {
			if err := node.ProposeAdd(ctx, group, uint64(i)); err != nil {
				t.Fatalf("ProposeAdd %d: %v", i, err)
			}
		}
		waitPersisted(t, st, group, n)
	}()

	// Second process: fresh engine, in-memory log — only the store can restore.
	st, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	eng := cardinality.NewEngine(hll.Algorithm{})
	restored := 0
	if err := st.LoadAll(func(g string, b []byte) error {
		restored++
		return eng.Restore(g, b)
	}); err != nil {
		t.Fatal(err)
	}
	if restored != 1 {
		t.Fatalf("restored %d groups, want 1", restored)
	}

	got, err := eng.Cardinality(group)
	if err != nil {
		t.Fatal(err)
	}
	if errPct(got, n) > 3 {
		t.Fatalf("after restart cardinality %d vs %d — %.2f%% error", got, n, errPct(got, n))
	}
	t.Logf("restart recovered cardinality=%d (expected≈%d, error=%.2f%%)", got, n, errPct(got, n))
}

func errPct(got uint64, want int) float64 {
	return math.Abs(float64(int64(got)-int64(want))) / float64(want) * 100
}

// waitPersisted polls the store until the group's persisted sketch holds
// ~want ids: ProposeAdd returns on acceptance, so apply+persist lag behind.
func waitPersisted(t *testing.T, st *store.BadgerStore, group string, want int) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	var last uint64
	for {
		if b, err := st.Load(group); err == nil {
			if sk, perr := (hll.Algorithm{}).Parse(b); perr == nil {
				last = sk.Cardinality()
				if errPct(last, want) <= 3 {
					return
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("persisted cardinality %d never reached ~%d", last, want)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
