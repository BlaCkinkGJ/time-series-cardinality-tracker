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
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	pb "github.com/BlaCkinkGJ/time-series-cardinality-tracker/gen/cardinality/v1"
	"github.com/BlaCkinkGJ/time-series-cardinality-tracker/internal/cardinality"
	"github.com/BlaCkinkGJ/time-series-cardinality-tracker/internal/cardinality/hll"
	"github.com/BlaCkinkGJ/time-series-cardinality-tracker/internal/store"
)

// getRequest builds a GET request for a handler test. http.NewRequestWithContext
// (not httptest.NewRequest, which noctx rejects) keeps the request cancellable.
func getRequest(t *testing.T, path string) *http.Request {
	t.Helper()

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, path, http.NoBody)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	return req
}

// scrape renders collectors as Prometheus text, the format users and
// Prometheus actually consume.
func scrape(t *testing.T, collectors ...prometheus.Collector) string {
	t.Helper()

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors...)

	rec := httptest.NewRecorder()
	promhttp.HandlerFor(reg, promhttp.HandlerOpts{}).
		ServeHTTP(rec, getRequest(t, "/metrics"))

	if rec.Code != http.StatusOK {
		t.Fatalf("scrape status = %d, want 200", rec.Code)
	}
	return rec.Body.String()
}

// lookup finds a sample by exact name line. A counter child that has
// never been incremented is not rendered at all, so absence is a valid
// zero for counters.
func lookup(body, name string) (float64, bool) {
	for _, line := range strings.Split(body, "\n") {
		rest, ok := strings.CutPrefix(line, name+" ")
		if !ok {
			continue
		}
		v, err := strconv.ParseFloat(strings.TrimSpace(rest), 64)
		if err != nil {
			return 0, false
		}
		return v, true
	}
	return 0, false
}

func sampleValue(t *testing.T, body, name string) float64 {
	t.Helper()

	v, ok := lookup(body, name)
	if !ok {
		t.Fatalf("metric %q not found in scrape:\n%s", name, body)
	}
	return v
}

func counterDelta(t *testing.T, before, after, name string) float64 {
	t.Helper()

	b, _ := lookup(before, name)
	a, _ := lookup(after, name)
	return a - b
}

func TestHealthzHandler(t *testing.T) {
	rec := httptest.NewRecorder()
	HealthzHandler()(rec, getRequest(t, "/healthz"))

	if rec.Code != http.StatusOK {
		t.Fatalf("healthz status = %d, want 200", rec.Code)
	}
}

func TestReadyzHandler(t *testing.T) {
	tests := []struct {
		name  string
		ready bool
		want  int
	}{
		{"not ready", false, http.StatusServiceUnavailable},
		{"ready", true, http.StatusOK},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			ReadyzHandler(func() bool { return tt.ready })(rec, getRequest(t, "/readyz"))

			if rec.Code != tt.want {
				t.Fatalf("readyz status = %d, want %d", rec.Code, tt.want)
			}
		})
	}
}

func TestEngineCollector(t *testing.T) {
	eng := cardinality.NewEngine(hll.Algorithm{})
	for i := 0; i < 5; i++ {
		if err := eng.Add("g-big", uint64(i)); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 3; i++ {
		if err := eng.Add("g-mid", uint64(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := eng.Add("g-small", 1); err != nil {
		t.Fatal(err)
	}

	t.Run("caps export and reports dropped groups", func(t *testing.T) {
		body := scrape(t, NewEngineCollector(eng, 2))

		if !strings.Contains(body, `cardinality_tracker_group_cardinality{group="g-big"}`) {
			t.Errorf("largest group missing:\n%s", body)
		}
		if !strings.Contains(body, `cardinality_tracker_group_cardinality{group="g-mid"}`) {
			t.Errorf("second largest group missing:\n%s", body)
		}
		if strings.Contains(body, `cardinality_tracker_group_cardinality{group="g-small"}`) {
			t.Errorf("export exceeded maxGroups=2:\n%s", body)
		}
		if got := sampleValue(t, body, "cardinality_tracker_groups_dropped"); got != 1 {
			t.Errorf("groups_dropped = %v, want 1", got)
		}
		if got := sampleValue(t, body, "cardinality_tracker_engine_groups"); got != 3 {
			t.Errorf("engine_groups = %v, want 3", got)
		}
	})

	t.Run("exports every group by default", func(t *testing.T) {
		body := scrape(t, NewEngineCollector(eng, 0))

		if !strings.Contains(body, `cardinality_tracker_group_cardinality{group="g-small"}`) {
			t.Errorf("group missing with unlimited export:\n%s", body)
		}
		if got := sampleValue(t, body, "cardinality_tracker_groups_dropped"); got != 0 {
			t.Errorf("groups_dropped = %v, want 0", got)
		}
	})
}

// TestBatchAddIsOneRequest guards the accounting contract: a batch of N
// ids is one BatchAdd request and one batch_size observation, no matter
// how many per-id proposals it fans out to. Before the addID extraction
// it also counted as N successful Add requests.
func TestBatchAddIsOneRequest(t *testing.T) {
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })

	eng := cardinality.NewEngine(hll.Algorithm{})
	srv := New(eng, st, nil, nil, "") // nil raft → standalone

	ids := make([]uint64, 0, 5)
	for i := 0; i < 5; i++ {
		ids = append(ids, uint64(i))
	}

	collectors := []prometheus.Collector{metricRequestsTotal, metricBatchSize}
	before := scrape(t, collectors...)

	if _, err := srv.BatchAdd(context.Background(), &pb.BatchAddRequest{Group: "g", Ids: ids}); err != nil {
		t.Fatalf("BatchAdd: %v", err)
	}

	after := scrape(t, collectors...)

	addDelta := counterDelta(t, before, after, `cardinality_tracker_requests_total{method="Add",status="success"}`)
	if addDelta != 0 {
		t.Errorf("Add counter grew by %v for a BatchAdd call, want 0", addDelta)
	}

	batchDelta := counterDelta(t, before, after, `cardinality_tracker_requests_total{method="BatchAdd",status="success"}`)
	if batchDelta != 1 {
		t.Errorf("BatchAdd counter grew by %v, want 1", batchDelta)
	}

	countDelta := counterDelta(t, before, after, "cardinality_tracker_batch_size_count")
	if countDelta != 1 {
		t.Errorf("batch_size observations grew by %v, want 1", countDelta)
	}

	sumDelta := counterDelta(t, before, after, "cardinality_tracker_batch_size_sum")
	if sumDelta != float64(len(ids)) {
		t.Errorf("batch_size sum grew by %v, want %d", sumDelta, len(ids))
	}
}
