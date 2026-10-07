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

package store

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Write-path instrumentation lives here, next to the write, because every
// caller (standalone server, Raft apply loop) goes through Save: one hook
// covers them all. Startup reads (Load/LoadAll) are one-shot and
// deliberately not instrumented.
var (
	metricSaveDurationSeconds = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "cardinality_tracker_store_save_duration_seconds",
			Help:    "Latency of BadgerDB sketch writes.",
			Buckets: prometheus.DefBuckets,
		},
	)

	metricSaveErrorsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "cardinality_tracker_store_save_errors_total",
			Help: "Total number of failed BadgerDB sketch writes.",
		},
	)
)
