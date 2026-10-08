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

package server

import (
	"context"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	pb "github.com/BlaCkinkGJ/time-series-cardinality-tracker/gen/cardinality/v1"
	"github.com/BlaCkinkGJ/time-series-cardinality-tracker/internal/cardinality"
	"github.com/BlaCkinkGJ/time-series-cardinality-tracker/internal/router"
	"github.com/BlaCkinkGJ/time-series-cardinality-tracker/internal/store"
)

// RaftNode is the minimal interface the server needs from the Raft layer.
// Implemented by *raft.Node in T7; nil means standalone mode.
type RaftNode interface {
	ProposeAdd(ctx context.Context, group string, id uint64) error
}

// Server implements pb.CardinalityServiceServer.
type Server struct {
	pb.UnimplementedCardinalityServiceServer
	engine   *cardinality.Engine
	store    *store.BadgerStore
	node     RaftNode
	router   *router.Ring // nil in standalone mode
	selfAddr string
	dialOpts []grpc.DialOption

	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

// New creates a Server. node, ring, selfAddr may be nil/empty for single-node operation.
func New(engine *cardinality.Engine, st *store.BadgerStore, node RaftNode, ring *router.Ring, selfAddr string) *Server {
	return &Server{
		engine:   engine,
		store:    st,
		node:     node,
		router:   ring,
		selfAddr: selfAddr,
		conns:    make(map[string]*grpc.ClientConn),
	}
}

// SetDialOptions configures custom gRPC dial options for peer forwarding connections.
func (s *Server) SetDialOptions(opts ...grpc.DialOption) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dialOpts = opts
}

func (s *Server) Add(ctx context.Context, req *pb.AddRequest) (*pb.AddResponse, error) {
	start := time.Now()
	var statusStr = "success"
	defer func() {
		metricRequestsTotal.WithLabelValues("Add", statusStr).Inc()
		metricRequestDurationSeconds.WithLabelValues("Add").Observe(time.Since(start).Seconds())
	}()

	if req.Group == "" {
		statusStr = "error"
		return nil, status.Error(codes.InvalidArgument, "group required")
	}

	if err := s.addID(ctx, "Add", req.Group, req.Id); err != nil {
		statusStr = "error"
		return nil, err
	}
	return &pb.AddResponse{Ok: true}, nil
}

// addID routes and applies a single id, and is the shared body of Add and
// BatchAdd: a batch must not be counted as len(ids) individual Add
// requests. method is the metric label of the originating request
// ("Add" or "BatchAdd"), so per-id work stays attributed to it.
//
// The id arrives as a uint64 and is passed through untouched: hashing for
// uniform distribution is the algorithm's job (hll hashes it, bitmap keeps
// it verbatim, so bitmap counts are exact over the caller's id space).
func (s *Server) addID(ctx context.Context, method, group string, id uint64) error {
	if s.router != nil && s.selfAddr != "" {
		owner := s.router.Resolve(group)
		if owner != "" && owner != s.selfAddr {
			metricForwardedRequestsTotal.WithLabelValues(owner, method).Inc()
			client, err := s.getPeerClient(owner)
			if err != nil {
				metricForwardedErrorsTotal.WithLabelValues(owner, method).Inc()
				return status.Errorf(codes.Internal, "dial peer %s: %v", owner, err)
			}
			_, err = client.Add(ctx, &pb.AddRequest{Group: group, Id: id})
			if err != nil {
				metricForwardedErrorsTotal.WithLabelValues(owner, method).Inc()
			}
			return err
		}
	}

	if s.node != nil {
		if err := s.node.ProposeAdd(ctx, group, id); err != nil {
			metricRaftProposalsTotal.WithLabelValues("error").Inc()
			return status.Errorf(codes.Internal, "raft propose: %v", err)
		}
		metricRaftProposalsTotal.WithLabelValues("success").Inc()
		return nil
	}

	if err := s.engine.AddAndPersist(group, id, func(b []byte) error {
		return s.store.Save(group, b)
	}); err != nil {
		return status.Errorf(codes.Internal, "engine add: %v", err)
	}
	return nil
}

func (s *Server) BatchAdd(ctx context.Context, req *pb.BatchAddRequest) (*pb.AddResponse, error) {
	start := time.Now()
	var statusStr = "success"
	defer func() {
		metricRequestsTotal.WithLabelValues("BatchAdd", statusStr).Inc()
		metricRequestDurationSeconds.WithLabelValues("BatchAdd").Observe(time.Since(start).Seconds())
	}()

	if req.Group == "" {
		statusStr = "error"
		return nil, status.Error(codes.InvalidArgument, "group required")
	}

	// BatchAdd fans out to single-id adds rather than proposing one
	// BATCH_ADD command per batch: each id is independently routed so
	// mixed-shard batches hit the correct owner without extra client
	// logic, and per-id errors surface as they happen. The per-id work
	// is attributed to BatchAdd, not to Add, so one batch stays one
	// observable request (see the batch_size histogram below).
	if s.router != nil && s.selfAddr != "" {
		owner := s.router.Resolve(req.Group)
		if owner != "" && owner != s.selfAddr {
			metricForwardedRequestsTotal.WithLabelValues(owner, "BatchAdd").Inc()
			client, err := s.getPeerClient(owner)
			if err != nil {
				metricForwardedErrorsTotal.WithLabelValues(owner, "BatchAdd").Inc()
				statusStr = "error"
				return nil, status.Errorf(codes.Internal, "dial peer %s: %v", owner, err)
			}
			resp, err := client.BatchAdd(ctx, req)
			if err != nil {
				metricForwardedErrorsTotal.WithLabelValues(owner, "BatchAdd").Inc()
				statusStr = "error"
			}
			return resp, err
		}
	}

	metricBatchSize.Observe(float64(len(req.Ids)))
	for _, v := range req.Ids {
		if err := s.addID(ctx, "BatchAdd", req.Group, v); err != nil {
			statusStr = "error"
			return nil, err
		}
	}
	return &pb.AddResponse{Ok: true}, nil
}

func (s *Server) Query(ctx context.Context, req *pb.QueryRequest) (*pb.QueryResponse, error) {
	start := time.Now()
	var statusStr = "success"
	defer func() {
		metricRequestsTotal.WithLabelValues("Query", statusStr).Inc()
		metricRequestDurationSeconds.WithLabelValues("Query").Observe(time.Since(start).Seconds())
	}()

	if req.Group == "" {
		statusStr = "error"
		return nil, status.Error(codes.InvalidArgument, "group required")
	}

	if s.router != nil && s.selfAddr != "" {
		owner := s.router.Resolve(req.Group)
		if owner != "" && owner != s.selfAddr {
			metricForwardedRequestsTotal.WithLabelValues(owner, "Query").Inc()
			client, err := s.getPeerClient(owner)
			if err != nil {
				metricForwardedErrorsTotal.WithLabelValues(owner, "Query").Inc()
				statusStr = "error"
				return nil, status.Errorf(codes.Internal, "dial peer %s: %v", owner, err)
			}
			resp, err := client.Query(ctx, req)
			if err != nil {
				metricForwardedErrorsTotal.WithLabelValues(owner, "Query").Inc()
				statusStr = "error"
			}
			return resp, err
		}
	}

	card, err := s.engine.Cardinality(req.Group)
	if err != nil {
		// Unknown group reads as empty, matching the pre-migration API.
		card = 0
	}
	return &pb.QueryResponse{Group: req.Group, Cardinality: card}, nil
}

func (s *Server) getPeerClient(addr string) (pb.CardinalityServiceClient, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	conn, ok := s.conns[addr]
	if !ok {
		var err error
		opts := append([]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}, s.dialOpts...)
		conn, err = grpc.Dial(addr, opts...)
		if err != nil {
			return nil, err
		}
		s.conns[addr] = conn
	}
	return pb.NewCardinalityServiceClient(conn), nil
}

func (s *Server) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, conn := range s.conns {
		_ = conn.Close()
	}
}
