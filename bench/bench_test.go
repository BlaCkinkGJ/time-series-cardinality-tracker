package bench_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/yourorg/cardinality-tracker/gen/cardinality/v1"
	"github.com/yourorg/cardinality-tracker/internal/hll"
	"github.com/yourorg/cardinality-tracker/internal/raft"
	"github.com/yourorg/cardinality-tracker/internal/router"
	"github.com/yourorg/cardinality-tracker/internal/server"
	"github.com/yourorg/cardinality-tracker/internal/store"
)

func BenchmarkHLL_Add(b *testing.B) {
	h := hll.New()
	val := []byte("benchmark-value")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.Add(val)
	}
}

func BenchmarkHLL_Estimate(b *testing.B) {
	h := hll.New()
	for i := 0; i < 100000; i++ {
		h.Add([]byte(fmt.Sprintf("v%d", i)))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.Estimate()
	}
}

func BenchmarkEngine_Add_Parallel(b *testing.B) {
	eng := hll.NewEngine()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			eng.Add("ts-bench", []byte(fmt.Sprintf("val-%d", i)))
			i++
		}
	})
}

func BenchmarkHLL_Marshal(b *testing.B) {
	h := hll.New()
	for i := 0; i < 100000; i++ {
		h.Add([]byte(fmt.Sprintf("v%d", i)))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := h.Marshal()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDistributed_Add_Forward(b *testing.B) {
	ctx := context.Background()
	numNodes := 3

	type testNode struct {
		addr string
		srv  *server.Server
		gs   *grpc.Server
		node *raft.Node
		dir  string
		st   *store.BadgerStore
	}
	nodes := make([]*testNode, numNodes)

	listeners := make([]net.Listener, numNodes)
	peerAddrs := make([]string, numNodes)
	for i := 0; i < numNodes; i++ {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			b.Fatal(err)
		}
		listeners[i] = lis
		peerAddrs[i] = lis.Addr().String()
	}

	ring := router.New()
	for _, addr := range peerAddrs {
		ring.AddNode(addr)
	}

	for i := 0; i < numNodes; i++ {
		dir, err := os.MkdirTemp("", fmt.Sprintf("dist-bench-node%d-*", i+1))
		if err != nil {
			b.Fatal(err)
		}
		st, err := store.Open(dir)
		if err != nil {
			b.Fatal(err)
		}
		eng := hll.NewEngine()
		nodeID := uint64(i + 1)
		node := raft.NewNode(nodeID, []raft.Peer{{ID: nodeID}}, eng, st)
		go node.Run()

		srv := server.New(eng, st, node, ring, peerAddrs[i])
		gs := grpc.NewServer()
		pb.RegisterCardinalityServiceServer(gs, srv)
		go gs.Serve(listeners[i])

		nodes[i] = &testNode{
			addr: peerAddrs[i],
			srv:  srv,
			gs:   gs,
			node: node,
			dir:  dir,
			st:   st,
		}
	}

	// Wait for Raft leaders to elect
	time.Sleep(2 * time.Second)

	clients := make([]pb.CardinalityServiceClient, numNodes)
	conns := make([]*grpc.ClientConn, numNodes)
	for i := 0; i < numNodes; i++ {
		conn, err := grpc.Dial(peerAddrs[i], grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			b.Fatal(err)
		}
		clients[i] = pb.NewCardinalityServiceClient(conn)
		conns[i] = conn
	}

	var globalCounter uint64
	b.ResetTimer()
	b.RunParallel(func(p *testing.PB) {
		for p.Next() {
			val := atomic.AddUint64(&globalCounter, 1)
			clientIdx := int(val) % numNodes
			seriesID := fmt.Sprintf("series-%d", val)
			_, err := clients[clientIdx].Add(ctx, &pb.AddRequest{
				SeriesId: seriesID,
				Value:    []byte(fmt.Sprintf("val-%d", val)),
			})
			if err != nil {
				b.Errorf("Add failed: %v", err)
			}
		}
	})
	b.StopTimer()

	for i := 0; i < numNodes; i++ {
		conns[i].Close()
		nodes[i].gs.Stop()
		nodes[i].node.Stop()
		nodes[i].srv.Close()
		nodes[i].st.Close()
		os.RemoveAll(nodes[i].dir)
	}
}

func BenchmarkDistributed_Add_Forward_Latency5ms(b *testing.B) {
	ctx := context.Background()
	numNodes := 3

	type testNode struct {
		addr string
		srv  *server.Server
		gs   *grpc.Server
		node *raft.Node
		dir  string
		st   *store.BadgerStore
	}
	nodes := make([]*testNode, numNodes)

	listeners := make([]net.Listener, numNodes)
	peerAddrs := make([]string, numNodes)
	for i := 0; i < numNodes; i++ {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			b.Fatal(err)
		}
		listeners[i] = lis
		peerAddrs[i] = lis.Addr().String()
	}

	ring := router.New()
	for _, addr := range peerAddrs {
		ring.AddNode(addr)
	}

	// Latency interceptor for client dials to simulate cross-node network delay
	simulatedLatency := 5 * time.Millisecond
	latencyOpt := grpc.WithUnaryInterceptor(func(
		ctx context.Context,
		method string,
		req, reply interface{},
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		time.Sleep(simulatedLatency)
		return invoker(ctx, method, req, reply, cc, opts...)
	})

	for i := 0; i < numNodes; i++ {
		dir, err := os.MkdirTemp("", fmt.Sprintf("dist-bench-lat-node%d-*", i+1))
		if err != nil {
			b.Fatal(err)
		}
		st, err := store.Open(dir)
		if err != nil {
			b.Fatal(err)
		}
		eng := hll.NewEngine()
		nodeID := uint64(i + 1)
		node := raft.NewNode(nodeID, []raft.Peer{{ID: nodeID}}, eng, st)
		go node.Run()

		srv := server.New(eng, st, node, ring, peerAddrs[i])
		srv.SetDialOptions(latencyOpt)

		gs := grpc.NewServer()
		pb.RegisterCardinalityServiceServer(gs, srv)
		go gs.Serve(listeners[i])

		nodes[i] = &testNode{
			addr: peerAddrs[i],
			srv:  srv,
			gs:   gs,
			node: node,
			dir:  dir,
			st:   st,
		}
	}

	// Wait for Raft leaders to elect
	time.Sleep(2 * time.Second)

	clients := make([]pb.CardinalityServiceClient, numNodes)
	conns := make([]*grpc.ClientConn, numNodes)
	for i := 0; i < numNodes; i++ {
		conn, err := grpc.Dial(peerAddrs[i], grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			b.Fatal(err)
		}
		clients[i] = pb.NewCardinalityServiceClient(conn)
		conns[i] = conn
	}

	var globalCounter uint64
	b.ResetTimer()
	b.RunParallel(func(p *testing.PB) {
		for p.Next() {
			val := atomic.AddUint64(&globalCounter, 1)
			clientIdx := int(val) % numNodes
			seriesID := fmt.Sprintf("series-%d", val)
			_, err := clients[clientIdx].Add(ctx, &pb.AddRequest{
				SeriesId: seriesID,
				Value:    []byte(fmt.Sprintf("val-%d", val)),
			})
			if err != nil {
				b.Errorf("Add failed: %v", err)
			}
		}
	})
	b.StopTimer()

	for i := 0; i < numNodes; i++ {
		conns[i].Close()
		nodes[i].gs.Stop()
		nodes[i].node.Stop()
		nodes[i].srv.Close()
		nodes[i].st.Close()
		os.RemoveAll(nodes[i].dir)
	}
}
