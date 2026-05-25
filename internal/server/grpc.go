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

	pb "github.com/yourorg/cardinality-tracker/gen/cardinality/v1"
	"github.com/yourorg/cardinality-tracker/internal/hll"
	"github.com/yourorg/cardinality-tracker/internal/router"
	"github.com/yourorg/cardinality-tracker/internal/store"
)

// RaftNode is the minimal interface the server needs from the Raft layer.
// Implemented by *raft.Node in T7; nil means standalone mode.
type RaftNode interface {
	ProposeAdd(ctx context.Context, seriesID string, value []byte) error
}

// Server implements pb.CardinalityServiceServer.
type Server struct {
	pb.UnimplementedCardinalityServiceServer
	engine   *hll.Engine
	store    *store.BadgerStore
	node     RaftNode
	router   *router.Ring // nil in standalone mode
	selfAddr string
	dialOpts []grpc.DialOption

	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

// New creates a Server. node, ring, selfAddr may be nil/empty for single-node operation.
func New(engine *hll.Engine, st *store.BadgerStore, node RaftNode, ring *router.Ring, selfAddr string) *Server {
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

	if req.SeriesId == "" {
		statusStr = "error"
		return nil, status.Error(codes.InvalidArgument, "series_id required")
	}
	if len(req.Value) == 0 {
		statusStr = "error"
		return nil, status.Error(codes.InvalidArgument, "value required")
	}

	if s.router != nil && s.selfAddr != "" {
		owner := s.router.Resolve(req.SeriesId)
		if owner != "" && owner != s.selfAddr {
			metricForwardedRequestsTotal.WithLabelValues(owner, "Add").Inc()
			client, err := s.getPeerClient(owner)
			if err != nil {
				statusStr = "error"
				return nil, status.Errorf(codes.Internal, "dial peer %s: %v", owner, err)
			}
			resp, err := client.Add(ctx, req)
			if err != nil {
				statusStr = "error"
			}
			return resp, err
		}
	}

	if s.node != nil {
		if err := s.node.ProposeAdd(ctx, req.SeriesId, req.Value); err != nil {
			statusStr = "error"
			metricRaftProposalsTotal.WithLabelValues("error").Inc()
			return nil, status.Errorf(codes.Internal, "raft propose: %v", err)
		}
		metricRaftProposalsTotal.WithLabelValues("success").Inc()
	} else {
		s.engine.Add(req.SeriesId, req.Value)
		if h, ok := s.engine.Get(req.SeriesId); ok {
			if err := s.store.Save(req.SeriesId, h); err != nil {
				statusStr = "error"
				return nil, status.Errorf(codes.Internal, "store save: %v", err)
			}
		}
	}
	return &pb.AddResponse{Ok: true}, nil
}

func (s *Server) BatchAdd(ctx context.Context, req *pb.BatchAddRequest) (*pb.AddResponse, error) {
	start := time.Now()
	var statusStr = "success"
	defer func() {
		metricRequestsTotal.WithLabelValues("BatchAdd", statusStr).Inc()
		metricRequestDurationSeconds.WithLabelValues("BatchAdd").Observe(time.Since(start).Seconds())
	}()

	if req.SeriesId == "" {
		statusStr = "error"
		return nil, status.Error(codes.InvalidArgument, "series_id required")
	}

	if s.router != nil && s.selfAddr != "" {
		owner := s.router.Resolve(req.SeriesId)
		if owner != "" && owner != s.selfAddr {
			metricForwardedRequestsTotal.WithLabelValues(owner, "BatchAdd").Inc()
			client, err := s.getPeerClient(owner)
			if err != nil {
				statusStr = "error"
				return nil, status.Errorf(codes.Internal, "dial peer %s: %v", owner, err)
			}
			resp, err := client.BatchAdd(ctx, req)
			if err != nil {
				statusStr = "error"
			}
			return resp, err
		}
	}

	for _, v := range req.Values {
		if _, err := s.Add(ctx, &pb.AddRequest{SeriesId: req.SeriesId, Value: v}); err != nil {
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

	if req.SeriesId == "" {
		statusStr = "error"
		return nil, status.Error(codes.InvalidArgument, "series_id required")
	}

	if s.router != nil && s.selfAddr != "" {
		owner := s.router.Resolve(req.SeriesId)
		if owner != "" && owner != s.selfAddr {
			metricForwardedRequestsTotal.WithLabelValues(owner, "Query").Inc()
			client, err := s.getPeerClient(owner)
			if err != nil {
				statusStr = "error"
				return nil, status.Errorf(codes.Internal, "dial peer %s: %v", owner, err)
			}
			resp, err := client.Query(ctx, req)
			if err != nil {
				statusStr = "error"
			}
			return resp, err
		}
	}

	est := s.engine.Estimate(req.SeriesId)
	return &pb.QueryResponse{SeriesId: req.SeriesId, Cardinality: est}, nil
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
		conn.Close()
	}
}
