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
For detailed design decisions, see [docs/architecture.md](docs/architecture.md) and [docs/explainable.md](docs/explainable.md).

```
                 +-----------------------+
                 |     HTTP / gRPC       |
                 |     API Gateway       |
                 +-----------+-----------+
                             |
             (Consistent Hashing Shard Routing)
                             |
         +-------------------+-------------------+
         |                   |                   |
         v                   v                   v
   +-----------+       +-----------+       +-----------+
   |  Node 1   |       |  Node 2   |       |  Node 3   |
   +-----+-----+       +-----+-----+       +-----+-----+
         |                   |                   |
    (Local Raft)        (Local Raft)        (Local Raft)
         |                   |                   |
         v                   v                   v
   +-----------+       +-----------+       +-----------+
   | BadgerDB  |       | BadgerDB  |       | BadgerDB  |
   +-----------+       +-----------+       +-----------+
```

1. **Shard Routing**: Each `series_id` is assigned to a node using a consistent hash ring (150 virtual nodes per node).
2. **Request Forwarding**: Nodes forward requests to the responsible peer over gRPC if they do not own the key.
3. **Local Durability**: Each node runs a single-node Raft group to manage local writes, snapshots, and disk persistence.

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

#### Add Value
```bash
curl -X POST http://localhost:8081/v1/series/prod-metrics/add \
  -d '{"value": "dXNlci0xMjM="}'
```

#### Batch Add Values
```bash
curl -X POST http://localhost:8081/v1/series/prod-metrics/batch \
  -d '{"values": ["dXNlci00NTY=", "dXNlci03ODk="]}'
```

#### Query Cardinality
```bash
curl http://localhost:8082/v1/series/prod-metrics/cardinality
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

Actual benchmark results on Apple Silicon (M-series / arm64):
```
goos: darwin
goarch: arm64
pkg: github.com/yourorg/cardinality-tracker/bench
BenchmarkHLL_Add-10                    	183289123	         6.445 ns/op	       0 B/op	       0 allocs/op
BenchmarkHLL_Estimate-10               	   12076	    102676 ns/op	       0 B/op	       0 allocs/op
BenchmarkEngine_Add_Parallel-10        	 9529302	       125.0 ns/op	      39 B/op	       2 allocs/op
BenchmarkHLL_Marshal-10                	 2885038	       428.2 ns/op	       0 B/op	       0 allocs/op
BenchmarkDistributed_Add_Forward-10    	   36922	     53455 ns/op	  241943 B/op	     366 allocs/op
```

## License
Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.
