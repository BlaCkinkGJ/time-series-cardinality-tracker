package server

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	metricRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cardinality_tracker_requests_total",
			Help: "Total number of gRPC requests handled by the service.",
		},
		[]string{"method", "status"},
	)

	metricRequestDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "cardinality_tracker_request_duration_seconds",
			Help:    "Latency histogram of gRPC requests handled by the service.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method"},
	)

	metricRaftProposalsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cardinality_tracker_raft_proposals_total",
			Help: "Total number of Raft proposals processed.",
		},
		[]string{"status"},
	)

	metricForwardedRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cardinality_tracker_forwarded_requests_total",
			Help: "Total number of forwarded requests sent to peer nodes.",
		},
		[]string{"peer", "method"},
	)
)
