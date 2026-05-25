# Time-Series Cardinality Tracker

A high-performance, sharded, and distributed time-series cardinality tracker built from scratch in Go. It handles millions of unique time-series ID additions and provides extremely fast, memory-efficient cardinality estimates with guaranteed scalability.

## Tech Stack
- **Go 1.21.3**
- **HyperLogLog++** (Engine for cardinalities with $<3\%$ error rates and low memory footprint)
- **etcd Raft v3** (Provides FSM log compaction, local durability, and WAL snapshots)
- **BadgerDB v4** (LSM-tree disk persistence for HLL sketches)
- **gRPC + grpc-gateway** (Provides unified high-performance internal RPC and RESTful external API)
- **murmur3 + Consistent Hashing** (Enables distributed horizontal scaling and request forwarding)

## Architecture Overview
For detailed design decisions and architecture diagrams, check the [docs/architecture.md](docs/architecture.md) and [docs/explainable.md](docs/explainable.md).

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
   | (Leader)  |       | (Leader)  |       | (Leader)  |
   +-----+-----+       +-----+-----+       +-----+-----+
         |                   |                   |
    (Local Raft)        (Local Raft)        (Local Raft)
         |                   |                   |
         v                   v                   v
   +-----------+       +-----------+       +-----------+
   | BadgerDB  |       | BadgerDB  |       | BadgerDB  |
   +-----------+       +-----------+       +-----------+
```

1. **Consistent Hash Sharding**: Every unique `series_id` is mapped to a specific node on a consistent-hash ring (150 virtual nodes per physical host).
2. **Request Forwarding**: Any node can accept a write or read request. If a node receives a key it doesn't own, it automatically forwards it over gRPC to the responsible peer.
3. **Local Durability**: Each node runs as a standalone, single-node Raft group to provide atomic append-only log ordering, snapshot compaction, and durable updates back to BadgerDB.

## Features
- **Ultra Fast**: HyperLogLog++ registers update in nanoseconds.
- **Scalable**: Horizontal scale-out by adding nodes to the consistent hash ring.
- **Durable**: Fully crash-safe via BadgerDB WAL persistence and Raft snapshots.
- **REST and gRPC**: Exposes HTTP/JSON REST routes via grpc-gateway alongside high-performance gRPC endpoints.

---

## Quick Start

### Prerequisites
- Docker & Docker Compose
- Go 1.21.3 (or later)

### Run in Docker (3-Node Cluster Setup)
Compile and launch a fully sharded 3-node cluster locally:
```bash
cd deploy
docker compose build
docker compose up -d
```
The nodes expose the following HTTP endpoints:
- Node 1: `http://localhost:8081`
- Node 2: `http://localhost:8082`
- Node 3: `http://localhost:8083`

### REST API Usage

#### 1. Add Value to a Series
Send a value to any node (e.g. `node1`):
```bash
# Add base64 encoded value "user-123" to series "prod-metrics"
curl -X POST http://localhost:8081/v1/series/prod-metrics/add \
  -d '{"value": "dXNlci0xMjM="}'
```

#### 2. Batch Add Multiple Values
```bash
curl -X POST http://localhost:8081/v1/series/prod-metrics/batch \
  -d '{"values": ["dXNlci00NTY=", "dXNlci03ODk="]}'
```

#### 3. Query Estimated Cardinality
Query from any peer (e.g. `node2`), which automatically resolves and retrieves the value from the owner node:
```bash
curl http://localhost:8082/v1/series/prod-metrics/cardinality
# Expected Output: {"seriesId":"prod-metrics","cardinality":"3"}
```

---

## Development

### Running Local Tests
Run all unit and in-memory integration tests:
```bash
# Unit tests
make test

# Integration tests
GOTOOLCHAIN=local CGO_ENABLED=0 go test ./... -tags=integration -count=1
```

### Running Benchmarks
Evaluate performance of the HyperLogLog engine under load:
```bash
make bench
```
*Expected throughput on typical ARM64 architectures:*
- HLL Add: $<10$ ns/op
- Parallel Engine Add: $<150$ ns/op
- HLL Marshalling: $<500$ ns/op

## License
Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.
