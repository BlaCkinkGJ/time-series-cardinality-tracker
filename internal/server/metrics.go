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

	// metricForwardedErrorsTotal counts forwarded requests that failed,
	// so attempts can be told apart from outcomes.
	metricForwardedErrorsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cardinality_tracker_forwarded_errors_total",
			Help: "Total number of forwarded requests that failed.",
		},
		[]string{"peer", "method"},
	)

	// metricBatchSize observes ids per BatchAdd call. One observation per
	// call, however many ids it carries.
	metricBatchSize = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "cardinality_tracker_batch_size",
			Help:    "Number of ids per BatchAdd request.",
			Buckets: []float64{1, 2, 5, 10, 50, 100, 500, 1000, 5000},
		},
	)

	// Transport-level view, filled by UnaryMetricsInterceptor: every RPC
	// with its real status code, including ones that never reach a handler.
	metricGRPCRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cardinality_tracker_grpc_requests_total",
			Help: "Total number of gRPC calls by method and status code.",
		},
		[]string{"method", "code"},
	)

	metricGRPCDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "cardinality_tracker_grpc_request_duration_seconds",
			Help:    "Latency of gRPC calls by method and status code.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method", "code"},
	)
)
