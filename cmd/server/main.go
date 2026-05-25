// cmd/server/main.go
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"

	pb "github.com/yourorg/cardinality-tracker/gen/cardinality/v1"
	"github.com/yourorg/cardinality-tracker/internal/hll"
	"github.com/yourorg/cardinality-tracker/internal/raft"
	"github.com/yourorg/cardinality-tracker/internal/router"
	"github.com/yourorg/cardinality-tracker/internal/server"
	"github.com/yourorg/cardinality-tracker/internal/store"
)

var (
	grpcPort  = flag.Int("grpc-port", 9090, "gRPC listen port")
	httpPort  = flag.Int("http-port", 8080, "HTTP gateway listen port")
	dataDir   = flag.String("data", "/tmp/cardinality-data", "BadgerDB data directory")
	nodeID    = flag.Uint64("node-id", 1, "Raft Node ID")
	peersFlag = flag.String("peers", "", "Comma-separated list of peers (host:port)")
)

func main() {
	flag.Parse()

	st, err := store.Open(*dataDir)
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	defer st.Close()

	eng := hll.NewEngine()

	var ring *router.Ring
	var selfAddr string

	if *peersFlag != "" {
		peerAddrs := strings.Split(*peersFlag, ",")
		ring = router.New()
		for _, addr := range peerAddrs {
			ring.AddNode(addr)
		}
		if *nodeID > 0 && int(*nodeID) <= len(peerAddrs) {
			selfAddr = peerAddrs[*nodeID-1]
		}
	}

	peers := []raft.Peer{{ID: *nodeID}}

	node := raft.NewNode(*nodeID, peers, eng, st)
	go node.Run()
	defer node.Stop()

	srv := server.New(eng, st, node, ring, selfAddr)
	defer srv.Close()

	// ── gRPC ──
	grpcLis, err := net.Listen("tcp", fmt.Sprintf(":%d", *grpcPort))
	if err != nil {
		log.Fatalf("listen grpc: %v", err)
	}
	gs := grpc.NewServer()
	pb.RegisterCardinalityServiceServer(gs, srv)
	reflection.Register(gs)
	go func() {
		log.Printf("gRPC listening on :%d", *grpcPort)
		if err := gs.Serve(grpcLis); err != nil {
			log.Printf("grpc serve: %v", err)
		}
	}()

	// ── HTTP gateway ──
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mux := runtime.NewServeMux()
	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	if err := pb.RegisterCardinalityServiceHandlerFromEndpoint(
		ctx, mux, fmt.Sprintf("localhost:%d", *grpcPort), opts,
	); err != nil {
		log.Fatalf("register gateway: %v", err)
	}

	httpSrv := &http.Server{Addr: fmt.Sprintf(":%d", *httpPort), Handler: mux}
	go func() {
		log.Printf("HTTP gateway listening on :%d", *httpPort)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("http serve: %v", err)
		}
	}()

	// ── Graceful shutdown ──
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Println("shutting down…")
	gs.GracefulStop()
	httpSrv.Shutdown(ctx) //nolint:errcheck
}
