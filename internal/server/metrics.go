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
)
