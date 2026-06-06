# Time-Series Cardinality Tracker

This is a sharded time-series cardinality tracker implemented in Go.

## Tech Stack
- **Go 1.21.3**
- **HyperLogLog++** (Engine for cardinalities with $p=14$)
- **etcd Raft v3** (Provides FSM log compaction and snapshotting)
- **BadgerDB v4** (LSM-tree disk persistence for HLL sketches)
- **gRPC + grpc-gateway** (Internal communication and HTTP REST API)
- **murmur3 + Consistent Hashing** (Shard routing and request forwarding)

## Architecture Overview
For detailed design decisions, see [docs/architecture.md](docs/architecture.md) and [docs/theory.md](docs/theory.md).

```
                 +-----------------------+
                 |     HTTP / gRPC       |
                 |   (Any Entry Node)    |
                 +-----------+-----------+
                             |
             [Consistent Hash: Resolve Shard]
                             |
                             v
              +------------------------------+
              |         Shard (Group)        |
              |    +--------------------+    |
              |    |    Raft Group      |    |
              |    | (Leader/Followers) |    |
              |    +---------+----------+    |
              +--------------|---------------+
                             |
         +-------------------+-------------------+
         |                   |                   |
         v                   v                   v
   +-----------+       +-----------+       +-----------+
   |  Node 1   |       |  Node 2   |       |  Node 3   |
   | (Replica) |       | (Replica) |       | (Replica) |
   | +-------+ |       | +-------+ |       | +-------+ |
   | |BadgerDB| |       | |BadgerDB| |       | |BadgerDB| |
   | +-------+ |       | +-------+ |       | +-------+ |
   +-----------+       +-----------+       +-----------+
```

1. **Shard Routing**: Each `group` maps to a **shard**. The shard's Raft group is replicated across 3+ nodes.
2. **Request Forwarding**: The consistent hash resolves the shard. Requests are routed to the leader node of that shard's Raft group.
3. **Local Durability**: Each **shard** has a Raft group. Proposals are written to the Raft log, applied to the in-memory HLL engine, and committed to BadgerDB on every replica node.

---

## Quick Start

### Run in Docker
```bash
cd deploy
docker compose build
docker compose up -d
```
Ports:
- Node 1 HTTP: `8081` (gRPC: `9091`)
- Node 2 HTTP: `8082` (gRPC: `9092`)
- Node 3 HTTP: `8083` (gRPC: `9093`)

### API Usage
For the complete API request/response definitions and gRPC payloads, refer to [docs/api-spec.md](docs/api-spec.md).

#### Add ID
```bash
curl -X POST http://localhost:8081/v1/group/prod-metrics/add \
  -d '{"id": "user-123"}'
```

#### Batch Add IDs
```bash
curl -X POST http://localhost:8081/v1/group/prod-metrics/batch \
  -d '{"ids": ["user-456", "user-789"]}'
```

#### Query Cardinality
```bash
curl http://localhost:8082/v1/group/prod-metrics/cardinality
```

---

## Development

### Run Tests
```bash
make test
GOTOOLCHAIN=local CGO_ENABLED=0 go test ./... -tags=integration -count=1
```

### Benchmarks
Run benchmarks using:
```bash
make bench
```

#### Benchmark Settings & Scenarios
- `BenchmarkHLL_Add`: Factual latency of updating a single local HLL++ sketch.
- `BenchmarkHLL_Estimate`: Factual latency of estimating cardinality for a series with 100k unique items. (Triggers the raw HLL mathematical estimation and bias-correction pipeline, rather than the small-cardinality Linear Counting optimization).
- `BenchmarkEngine_Add_Parallel`: Concurrent throughput test updating a shared, thread-safe local engine map.
- `BenchmarkHLL_Marshal`: Factual latency of encoding the HLL register state.
- `BenchmarkDistributed_Add_Forward`: Local simulation of a 3-node sharded cluster. Measures the overhead of consistent-hash lookup and gRPC request sharding/forwarding to peer nodes.
- `BenchmarkDistributed_Add_Forward_Latency5ms`: 3-node cluster simulation with an artificial 5ms network delay injected into peer gRPC connection dials to simulate cross-host/cross-AZ network latency.

Actual benchmark results on Apple Silicon (M-series / arm64):
```
goos: darwin
goarch: arm64
pkg: github.com/yourorg/cardinality-tracker/bench
BenchmarkHLL_Add-10                            	184483040	         6.557 ns/op	       0 B/op	       0 allocs/op
BenchmarkHLL_Estimate-10                       	    4857	    227917 ns/op	       0 B/op	       0 allocs/op
BenchmarkEngine_Add_Parallel-10                	 9261643	       131.2 ns/op	      39 B/op	       2 allocs/op
BenchmarkHLL_Marshal-10                        	 2628984	       433.7 ns/op	       0 B/op	       0 allocs/op
BenchmarkDistributed_Add_Forward-10            	   44205	     44285 ns/op	  233215 B/op	     366 allocs/op
BenchmarkDistributed_Add_Forward_Latency5ms-10 	    2845	    406251 ns/op	   53800 B/op	     368 allocs/op
```

## License
Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.
