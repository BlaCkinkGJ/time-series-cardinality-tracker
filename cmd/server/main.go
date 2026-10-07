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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"

	pb "github.com/BlaCkinkGJ/time-series-cardinality-tracker/gen/cardinality/v1"
	"github.com/BlaCkinkGJ/time-series-cardinality-tracker/internal/cardinality"
	"github.com/BlaCkinkGJ/time-series-cardinality-tracker/internal/cardinality/hll"
	"github.com/BlaCkinkGJ/time-series-cardinality-tracker/internal/raft"
	"github.com/BlaCkinkGJ/time-series-cardinality-tracker/internal/router"
	"github.com/BlaCkinkGJ/time-series-cardinality-tracker/internal/server"
	"github.com/BlaCkinkGJ/time-series-cardinality-tracker/internal/store"
)

var (
	grpcPort         = flag.Int("grpc-port", 9090, "gRPC listen port")
	httpPort         = flag.Int("http-port", 8080, "HTTP gateway listen port")
	metricsPort      = flag.Int("metrics-port", 8081, "metrics and health listen port")
	metricsMaxGroups = flag.Int("metrics-max-groups", 1000, "max groups exported by the per-group cardinality metric (0 = unlimited)")
	dataDir          = flag.String("data", "/tmp/cardinality-data", "BadgerDB data directory")
	nodeID           = flag.Uint64("node-id", 1, "Raft Node ID")
	peersFlag        = flag.String("peers", "", "Comma-separated list of peers (host:port)")
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

	eng := cardinality.NewEngine(hll.Algorithm{})

	// Rebuild state from disk before serving: every applied entry was
	// persisted, and the Raft log itself is in-memory.
	restored := 0
	if err := st.LoadAll(func(group string, b []byte) error {
		if err := eng.Restore(group, b); err != nil {
			return err
		}
		restored++
		return nil
	}); err != nil {
		return fmt.Errorf("restore engine state failed: %w", err)
	}
	slog.Info("restored engine state", "groups", restored)

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

	prometheus.MustRegister(
		server.NewEngineCollector(eng, *metricsMaxGroups),
		server.NewRaftCollector(node),
	)

	// Ready means "this node can answer correct reads": it is the Raft
	// leader of its group. State restore happens before any listener
	// opens, so it cannot be observed through /readyz and is not part of
	// this predicate. Single-node groups elect within ~1s.
	ready := func() bool {
		_, isLeader, _ := node.Status()
		return isLeader
	}

	srv := server.New(eng, st, node, ring, selfAddr)
	defer srv.Close()

	// ── gRPC ──
	grpcLis, err := net.Listen("tcp", fmt.Sprintf(":%d", *grpcPort))
	if err != nil {
		return fmt.Errorf("listen grpc failed: %w", err)
	}
	gs := grpc.NewServer(grpc.UnaryInterceptor(server.UnaryMetricsInterceptor))
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
	// Health also answers on the gateway port so external load balancers
	// and humans can probe a node without reaching the metrics listener.
	// /metrics deliberately does not: it stays off the public surface.
	mainMux.HandleFunc("/healthz", server.HealthzHandler())
	mainMux.HandleFunc("/readyz", server.ReadyzHandler(ready))
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

	// ── Metrics + health (not exposed by the public services) ──
	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.Handler())
	metricsMux.HandleFunc("/healthz", server.HealthzHandler())
	metricsMux.HandleFunc("/readyz", server.ReadyzHandler(ready))

	metricsSrv := &http.Server{
		Addr:              fmt.Sprintf(":%d", *metricsPort),
		Handler:           metricsMux,
		ReadHeaderTimeout: 3 * time.Second,
	}
	go func() {
		slog.Info("metrics listening", "port", *metricsPort, "max_groups", *metricsMaxGroups)
		if err := metricsSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("metrics serve failed", "error", err)
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
	if err := metricsSrv.Shutdown(shutdownCtx); err != nil {
		slog.Error("metrics shutdown failed", "error", err)
	}
	return nil
}
