# AGENTS.md

## Project Overview

**Time-Series Cardinality Tracker** — A sharded time-series cardinality tracker implemented in Go.

Estimates the number of unique values (cardinality) in time-series data using pluggable cardinality sketches (HyperLogLog++ by default, Roaring64 bitmap), with distributed coordination via Raft consensus.

## Tech Stack

- **Go 1.21.3**
- **Pluggable cardinality engine** — `internal/cardinality` (`Sketch`/`Algorithm` interfaces) with `hll` (HLL++ p=14) and `bitmap` (Roaring64) backends
- **etcd Raft v3** — log replication, compaction, snapshotting
- **BadgerDB v4** — opaque serialised sketches at key prefix `sketch/<group>`
- **gRPC + grpc-gateway** — internal communication and HTTP REST API
- **murmur3** — HLL register hashing and consistent-hash shard routing

## Architecture

```
HTTP / gRPC → Consistent Hash (Shard Routing) → Raft Group (Leader/Followers) → Cardinality Engine → BadgerDB
```

- **Shard Routing**: Each `group` maps to a shard. The shard's Raft group is replicated across 3+ nodes.
- **Write path (Raft)**: `Node.ProposeAdd` marshals a `pb.Command{type, group, payload}` into the Raft log → apply loop → `handler.Registry.Dispatch` → `cardinality.Engine` → `BadgerStore.Save`. Every replica applies the same entry.
- **Standalone path**: with `-peers` empty there is no Raft node (`Server.node == nil`) and the gRPC handler calls `Engine.AddAndPersist` directly.
- **Durability**: `Engine.AddAndPersist` / `Engine.Persist` hold the engine write lock across serialise **and** save, so no caller ever handles raw sketch bytes.
- **Startup restore**: the Raft log is `etcdraft.MemoryStorage`, so state comes back from Badger — `cmd/server` runs `store.LoadAll(engine.Restore)` before serving. Replaying an id is idempotent, so starting from an empty log is safe.

## Project Structure

```
├── cmd/server/                  # Entry point: wires Engine + Raft + gRPC (see Server Flags)
├── internal/
│   ├── cardinality/             # Pluggable engine: Sketch/Algorithm interfaces + per-group Engine
│   │   ├── hll/                 # HLL++ (p=14, 16384 registers) — the backend cmd/server uses
│   │   └── bitmap/              # Roaring64 bitmap (exact counts)
│   ├── raft/                    # Raft node: apply loop, snapshotting (node.go)
│   │   └── handler/             # Generic WAL: command registry + ADD/BATCH_ADD/MERGE_SKETCH
│   ├── router/                  # Consistent hash shard routing
│   ├── server/                  # gRPC service, grpc-gateway, metrics, integration tests
│   └── store/                   # BadgerDB persistence (opaque bytes at sketch/<group>)
├── proto/cardinality/v1/        # cardinality.proto (API) + command.proto (WAL Command)
├── gen/                         # Generated gRPC/gateway code — never edit by hand, run `make proto`
├── deploy/
│   ├── docker/                  # Dockerfile + docker-compose.yml (3 nodes)
│   └── kubernetes/              # K8s manifests (StatefulSet, Services, ServiceMonitor)
├── bench/                       # Benchmark tests
├── docs/                        # Architecture, API spec, deployment, theory
├── scripts/                     # smoke-test.sh (3-node docker cluster)
└── third_party/                 # google/api protos required by grpc-gateway
```

## Development Commands

### Build
```bash
make build
# or
go build ./...
```

### Test

File-scoped first — one package or one test is enough while iterating:

```bash
go test ./internal/raft/...                                  # one package
go test -run TestEngine_MergeBytes ./internal/cardinality/   # one test
golangci-lint run ./internal/server/...                      # one package
```

Full suite (what CI runs) before handing work back:

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

### Smoke Test
```bash
./scripts/smoke-test.sh   # docker compose 3-node cluster; tears down on exit
```

### Server Flags

| Flag | Default | Meaning |
|------|---------|---------|
| `-http-port` | `8080` | HTTP gateway listen port |
| `-grpc-port` | `9090` | gRPC listen port |
| `-data` | `/tmp/cardinality-data` | BadgerDB data directory |
| `-node-id` | `1` | Raft node ID |
| `-peers` | `""` | Comma-separated `host:port` peers; **empty = standalone (no Raft)** |

## Extending

### Add a WAL command (no proto or enum change)

The WAL is generic: `Command.type` is a string and `payload` is opaque bytes, so a new command needs no proto edit.

1. `internal/raft/handler/<name>.go` — `const TypeX = "X"`, `func applyX(cmd *pb.Command, apply Adder) error`, and `func RegisterX(r *Registry) { r.Register(TypeX, applyX) }`.
2. Call `RegisterX(r)` inside `DefaultRegistry()` (`internal/raft/handler/handler.go`).
3. Add a proposer that marshals `pb.Command{Type: TypeX, ...}` (see `Node.ProposeAdd`).

The payload schema is agreed per type and decoded only by its handler; a malformed payload returns `ErrBadPayload`.

### Add an algorithm

1. Implement `cardinality.Sketch` + `cardinality.Algorithm` (`AlgoName()` must equal `Name()`); `internal/cardinality/bitmap` is the smallest example.
2. Pass it to `cardinality.NewEngine(alg)` at the wiring point (`cmd/server/main.go`). One algorithm per Engine; merging a foreign sketch returns `cardinality.ErrAlgoMismatch`.

## Docker

```bash
cd deploy/docker
docker compose build
docker compose up -d
```

Ports:
- Node 1: HTTP `8081`, gRPC `9091`, metrics/health `8181`
- Node 2: HTTP `8082`, gRPC `9092`, metrics/health `8182`
- Node 3: HTTP `8083`, gRPC `9093`, metrics/health `8183`

`/metrics`, `/healthz` and `/readyz` are served on the metrics port only
(`-metrics-port`, default 8081); the public HTTP gateway never exposes
`/metrics`.

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
- New files carry the repo's Apache 2.0 header (see any file under `internal/`)
- `Sketch` implementations need not be concurrency-safe — the per-group `Engine` serialises access

## Permissions

- **Autonomous** — reading files, `gofmt` / `go vet` / `golangci-lint`, running tests, editing source and docs.
- **Ask first** — dependency changes (`go get`, `go.mod`), `git push` and branch deletion, deleting files, anything under `deploy/`, and docker/k8s commands.
- **Never** — commit credentials, `.env` files, or a `-data` directory.

## CI

`.github/workflows/ci.yml` runs: `lint`, `license-check`, `test` (unit + integration), `smoke-test` (docker), `validate-k8s`, `validate-docker`.

## Key Design Decisions

- **One algorithm per `Engine`** (`NewEngine(alg)`). There is no `Register`/`Get` name registry, so per-group algorithm selection does not exist.
- **HLL++ p=14** gives 16384 registers (~16 KB per sketch) at ~0.81% standard error. `Add` hashes the **decimal** form of the id with murmur3, matching the pre-migration raft path so raft-written sketches keep their counts.
- **`bitmap` is Roaring64**, i.e. compressed — exact counts, not a dense `max_id/8` bitset.
- **Badger stores opaque sketch bytes** at `sketch/<group>`; the store knows nothing about HLL or roaring.
- **Raft for consistency, standalone for single-node**; both paths persist through the engine, never around it.
- **Snapshot payload** is `Engine.Marshal` (gob of every group), handed to `storage.ApplySnapshot`.
- **Consistent hashing** keeps reshuffling minimal on topology changes; each shard is managed by its own Raft group.

## Known Gaps

Verified against the current tree — do not assume otherwise:

- **`BATCH_ADD` and `MERGE_SKETCH` are never proposed.** Both handlers are registered and unit-tested, but `ProposeAdd` only emits `ADD` and `Server.BatchAdd` fans out to single `ADD`s. — **#22**
- **`bitmap` is unreachable at runtime** — `cmd/server` wires HLL only; the bitmap backend is exercised by tests alone. — **#11**
- **Proto `id`/`ids` are `string`**; hashing to `uint64` happens in `internal/server` (`hashID`). — **#21**

## References

- [Architecture](docs/architecture.md)
- [Theory](docs/theory.md)
- [API Spec](docs/api-spec.md)
- [Deployment](docs/deployment.md)
