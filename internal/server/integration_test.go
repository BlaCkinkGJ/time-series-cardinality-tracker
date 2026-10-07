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
	"fmt"
	"math"
	"net"
	"os"
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

func TestIntegration_AddQuery_WithRaft(t *testing.T) {
	dir, err := os.MkdirTemp("", "int-test-*")
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

	// Wait for leader election
	time.Sleep(600 * time.Millisecond)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lis.Close()

	srv := server.New(eng, st, node, nil, "")
	gs := grpc.NewServer()
	pb.RegisterCardinalityServiceServer(gs, srv)
	go gs.Serve(lis)
	defer gs.Stop()

	conn, err := grpc.Dial(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	client := pb.NewCardinalityServiceClient(conn)
	ctx := context.Background()

	n := 50000
	for i := 0; i < n; i++ {
		_, err := client.Add(ctx, &pb.AddRequest{Group: "prod-group", Id: fmt.Sprintf("u%d", i)})
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
