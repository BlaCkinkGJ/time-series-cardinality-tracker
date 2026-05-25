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

package server_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	pb "github.com/yourorg/cardinality-tracker/gen/cardinality/v1"
	"github.com/yourorg/cardinality-tracker/internal/hll"
	"github.com/yourorg/cardinality-tracker/internal/server"
	"github.com/yourorg/cardinality-tracker/internal/store"
)

const bufSize = 1 << 20

func newTestServer(t *testing.T) (pb.CardinalityServiceClient, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "srv-test-*")
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	eng := hll.NewEngine()
	srv := server.New(eng, st, nil, nil, "") // nil raft → standalone

	lis := bufconn.Listen(bufSize)
	gs := grpc.NewServer()
	pb.RegisterCardinalityServiceServer(gs, srv)
	go gs.Serve(lis) //nolint:errcheck

	conn, err := grpc.DialContext(
		context.Background(), "bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}

	return pb.NewCardinalityServiceClient(conn), func() {
		conn.Close()
		gs.Stop()
		st.Close()
		os.RemoveAll(dir)
	}
}

func TestAddAndQuery(t *testing.T) {
	client, cleanup := newTestServer(t)
	defer cleanup()
	ctx := context.Background()

	for i := 0; i < 1000; i++ {
		_, err := client.Add(ctx, &pb.AddRequest{
			Group: "my-group",
			Id:    []byte(fmt.Sprintf("user-%d", i)),
		})
		if err != nil {
			t.Fatalf("Add %d: %v", i, err)
		}
	}

	resp, err := client.Query(ctx, &pb.QueryRequest{Group: "my-group", StaleOk: true})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Cardinality < 900 || resp.Cardinality > 1100 {
		t.Fatalf("cardinality %d out of expected [900,1100]", resp.Cardinality)
	}
}

func TestAdd_ValidationErrors(t *testing.T) {
	client, cleanup := newTestServer(t)
	defer cleanup()
	ctx := context.Background()

	_, err := client.Add(ctx, &pb.AddRequest{Group: "", Id: []byte("x")})
	if err == nil {
		t.Fatal("expected error for empty group")
	}

	_, err = client.Add(ctx, &pb.AddRequest{Group: "g", Id: nil})
	if err == nil {
		t.Fatal("expected error for empty id")
	}
}

func TestBatchAdd(t *testing.T) {
	client, cleanup := newTestServer(t)
	defer cleanup()
	ctx := context.Background()

	ids := make([][]byte, 500)
	for i := range ids {
		ids[i] = []byte(fmt.Sprintf("batch-%d", i))
	}
	_, err := client.BatchAdd(ctx, &pb.BatchAddRequest{Group: "batch-group", Ids: ids})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := client.Query(ctx, &pb.QueryRequest{Group: "batch-group", StaleOk: true})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Cardinality < 450 || resp.Cardinality > 550 {
		t.Fatalf("batch cardinality %d out of [450,550]", resp.Cardinality)
	}
}
