// internal/server/grpc.go
package server

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/yourorg/cardinality-tracker/gen/cardinality/v1"
	"github.com/yourorg/cardinality-tracker/internal/hll"
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
	engine *hll.Engine
	store  *store.BadgerStore
	node   RaftNode // nil in standalone mode
}

// New creates a Server. node may be nil for single-node operation.
func New(engine *hll.Engine, st *store.BadgerStore, node RaftNode) *Server {
	return &Server{engine: engine, store: st, node: node}
}

func (s *Server) Add(ctx context.Context, req *pb.AddRequest) (*pb.AddResponse, error) {
	if req.SeriesId == "" {
		return nil, status.Error(codes.InvalidArgument, "series_id required")
	}
	if len(req.Value) == 0 {
		return nil, status.Error(codes.InvalidArgument, "value required")
	}
	if s.node != nil {
		if err := s.node.ProposeAdd(ctx, req.SeriesId, req.Value); err != nil {
			return nil, status.Errorf(codes.Internal, "raft propose: %v", err)
		}
	} else {
		s.engine.Add(req.SeriesId, req.Value)
		if h, ok := s.engine.Get(req.SeriesId); ok {
			if err := s.store.Save(req.SeriesId, h); err != nil {
				return nil, status.Errorf(codes.Internal, "store save: %v", err)
			}
		}
	}
	return &pb.AddResponse{Ok: true}, nil
}

func (s *Server) BatchAdd(ctx context.Context, req *pb.BatchAddRequest) (*pb.AddResponse, error) {
	if req.SeriesId == "" {
		return nil, status.Error(codes.InvalidArgument, "series_id required")
	}
	for _, v := range req.Values {
		if _, err := s.Add(ctx, &pb.AddRequest{SeriesId: req.SeriesId, Value: v}); err != nil {
			return nil, err
		}
	}
	return &pb.AddResponse{Ok: true}, nil
}

func (s *Server) Query(ctx context.Context, req *pb.QueryRequest) (*pb.QueryResponse, error) {
	if req.SeriesId == "" {
		return nil, status.Error(codes.InvalidArgument, "series_id required")
	}
	est := s.engine.Estimate(req.SeriesId)
	return &pb.QueryResponse{SeriesId: req.SeriesId, Cardinality: est}, nil
}
