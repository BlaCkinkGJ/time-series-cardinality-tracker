# AGENTS.md

## Project Overview

**Time-Series Cardinality Tracker** — A sharded time-series cardinality tracker implemented in Go.

Estimates the number of unique values (cardinality) in time-series data using HyperLogLog++ (HLL++) sketches, with distributed coordination via Raft consensus.

## Tech Stack

- **Go 1.21.3**
- **HyperLogLog++** — Cardinality estimation engine (precision p=14)
- **etcd Raft v3** — FSM log compaction and snapshotting
- **BadgerDB v4** — LSM-tree disk persistence for HLL sketches
- **gRPC + grpc-gateway** — Internal communication and HTTP REST API
- **murmur3 + Consistent Hashing** — Shard routing and request forwarding

## Architecture

```
HTTP / gRPC → Consistent Hash (Shard Routing) → Raft Group (Leader/Followers) → HLL Engine + BadgerDB
```

- **Shard Routing**: Each `group` maps to a shard. The shard's Raft group is replicated across 3+ nodes.
- **Request Forwarding**: The consistent hash resolves the shard. Requests are routed to the leader node.
- **Local Durability**: Each shard has a Raft group. Proposals are written to the Raft log, applied to the in-memory HLL engine, and committed to BadgerDB on every replica.

## Project Structure

```
├── cmd/server/          # Entry point (main.go)
├── internal/
│   ├── hll/             # HyperLogLog++ engine
│   ├── raft/            # Raft consensus (FSM, node management)
│   ├── router/          # Consistent hash shard routing
│   ├── server/          # gRPC server, metrics, integration tests
│   └── store/           # BadgerDB persistence layer
├── proto/               # Protocol Buffer definitions
├── gen/                 # Generated gRPC/gateway code
├── deploy/
│   ├── docker/          # Dockerfile + docker-compose.yml
│   └── kubernetes/      # K8s manifests (StatefulSet, Services, ServiceMonitor)
├── bench/               # Benchmark tests
├── docs/                # Architecture, API spec, deployment, theory docs
├── scripts/             # smoke-test.sh
└── third_party/         # External proto dependencies
```

## Development Commands

### Build
```bash
make build
# or
go build ./...
```

### Test
```bash
make test
# or with integration tests
GOTOOLCHAIN=local CGO_ENABLED=0 go test ./... -tags=integration -count=1
```

### Benchmarks
```bash
make bench
# or
go test ./bench/... -bench=. -benchmem -run=^$
```

### Lint
```bash
make lint
# or
golangci-lint run
```

### Protobuf Generation
```bash
make proto
```

## Docker

```bash
cd deploy/docker
docker compose build
docker compose up -d
```

Ports:
- Node 1: HTTP `8081`, gRPC `9091`
- Node 2: HTTP `8082`, gRPC `9092`
- Node 3: HTTP `8083`, gRPC `9093`

## API Examples

### Add ID
```bash
curl -X POST http://localhost:8081/v1/group/prod-metrics/add \
  -d '{"id": "user-123"}'
```

### Batch Add IDs
```bash
curl -X POST http://localhost:8081/v1/group/prod-metrics/batch \
  -d '{"ids": ["user-456", "user-789"]}'
```

### Query Cardinality
```bash
curl http://localhost:8082/v1/group/prod-metrics/cardinality
```

## Code Conventions

- Use `CGO_ENABLED=0` for builds (static binaries)
- Set `GOTOOLCHAIN=local` to avoid auto-downloading Go versions
- Integration tests use `-tags=integration` build tag
- Follow standard Go project layout (`cmd/`, `internal/`, `proto/`)
- Protobuf definitions in `proto/`, generated code in `gen/`

## Key Design Decisions

- HLL++ with p=14 gives ~16K registers (16,384) — good balance of accuracy and memory
- Raft provides strong consistency for cardinality updates across replicas
- BadgerDB persists HLL sketches to survive restarts without losing state
- Consistent hashing distributes load evenly across shards with minimal reshuffling on topology changes
- Each shard is independently managed by its own Raft group

## References

- [Architecture](docs/architecture.md)
- [Theory](docs/theory.md)
- [API Spec](docs/api-spec.md)
- [Deployment](docs/deployment.md)
