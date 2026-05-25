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
	"net"
	"os"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/yourorg/cardinality-tracker/gen/cardinality/v1"
	"github.com/yourorg/cardinality-tracker/internal/hll"
	"github.com/yourorg/cardinality-tracker/internal/raft"
	"github.com/yourorg/cardinality-tracker/internal/server"
	"github.com/yourorg/cardinality-tracker/internal/store"
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

	eng := hll.NewEngine()

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
		_, err := client.Add(ctx, &pb.AddRequest{SeriesId: "prod-series", Value: []byte(fmt.Sprintf("u%d", i))})
		if err != nil {
			t.Fatalf("Add %d: %v", i, err)
		}
	}

	resp, err := client.Query(ctx, &pb.QueryRequest{SeriesId: "prod-series", StaleOk: true})
	if err != nil {
		t.Fatal(err)
	}

	errPct := float64(int64(resp.Cardinality)-int64(n)) / float64(n) * 100
	if errPct < -3 || errPct > 3 {
		t.Fatalf("cardinality %d vs %d — %.2f%% error", resp.Cardinality, n, errPct)
	}
	t.Logf("cardinality=%d (expected≈%d, error=%.2f%%)", resp.Cardinality, n, errPct)
}
