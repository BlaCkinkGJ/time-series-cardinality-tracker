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
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

// UnaryMetricsInterceptor records transport-level outcomes for every
// unary RPC, with the real gRPC status code rather than a hand-set
// success/error string, and including calls that fail before reaching a
// handler. Pair it with metricRequestsTotal / metricRequestDurationSeconds,
// which measure the handler's own view of a request.
func UnaryMetricsInterceptor(
	ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler,
) (any, error) {
	start := time.Now()
	resp, err := handler(ctx, req)
	code := status.Code(err).String()

	metricGRPCRequestsTotal.WithLabelValues(info.FullMethod, code).Inc()
	metricGRPCDurationSeconds.WithLabelValues(info.FullMethod, code).Observe(time.Since(start).Seconds())

	return resp, err
}
