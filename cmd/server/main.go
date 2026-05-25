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

package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/prometheus/client_golang/prometheus/promhttp"
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

	if err := run(); err != nil {
		slog.Error("server run failed", "error", err)
		os.Exit(1)
	}
}

func run() error {
	st, err := store.Open(*dataDir)
	if err != nil {
		return fmt.Errorf("open store failed: %w", err)
	}
	defer func() { _ = st.Close() }()

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
		return fmt.Errorf("listen grpc failed: %w", err)
	}
	gs := grpc.NewServer()
	pb.RegisterCardinalityServiceServer(gs, srv)
	reflection.Register(gs)
	go func() {
		slog.Info("gRPC listening", "port", *grpcPort)
		if err := gs.Serve(grpcLis); err != nil {
			slog.Error("grpc serve failed", "error", err)
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
		return fmt.Errorf("register gateway failed: %w", err)
	}

	mainMux := http.NewServeMux()
	mainMux.Handle("/metrics", promhttp.Handler())
	mainMux.Handle("/", mux)

	httpSrv := &http.Server{
		Addr:              fmt.Sprintf(":%d", *httpPort),
		Handler:           mainMux,
		ReadHeaderTimeout: 3 * time.Second,
	}
	go func() {
		slog.Info("HTTP gateway listening", "port", *httpPort)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("http serve failed", "error", err)
		}
	}()

	// ── Graceful shutdown ──
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	slog.Info("shutting down…")
	gs.GracefulStop()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		slog.Error("HTTP gateway shutdown failed", "error", err)
	}
	return nil
}
