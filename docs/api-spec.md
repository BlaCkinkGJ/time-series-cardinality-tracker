# API Specification

This document defines the REST and gRPC API specifications for the Group Cardinality Tracker.

---

## 1. REST API (HTTP/JSON)

The HTTP REST API is exposed by the HTTP gateway. All request values (`id` or `ids`) sent via JSON are plain strings. How a string reaches a sketch is fixed, not an implementation detail: see [§1.5 ID Semantics](#15-id-semantics).

### 1.1 Add ID
Adds a single unique item to a group HLL sketch.

- **Method**: `POST`
- **Path**: `/v1/group/{group}/add`
- **Headers**:
  - `Content-Type: application/json`
- **URL Parameters**:
  - `group` (string): Unique identifier for the group.
- **Request Body**:
  ```json
  {
    "id": "<id_string>"
  }
  ```
- **Response Body**:
  ```json
  {
    "ok": true
  }
  ```

#### Example Request
```bash
curl -X POST http://localhost:8081/v1/group/sensor-01/add \
  -H "Content-Type: application/json" \
  -d '{"id": "user-123"}'
```

---

### 1.2 Batch Add IDs
Adds multiple unique items to a group HLL sketch in a single request.

- **Method**: `POST`
- **Path**: `/v1/group/{group}/batch`
- **Headers**:
  - `Content-Type: application/json`
- **URL Parameters**:
  - `group` (string): Unique identifier for the group.
- **Request Body**:
  ```json
  {
    "ids": [
      "<id_string_1>",
      "<id_string_2>"
    ]
  }
  ```
- **Response Body**:
  ```json
  {
    "ok": true
  }
  ```

#### Example Request
```bash
curl -X POST http://localhost:8081/v1/group/sensor-01/batch \
  -H "Content-Type: application/json" \
  -d '{"ids": ["user-456", "user-789"]}'
```

---

### 1.3 Query Cardinality
Retrieves the estimated unique item count (cardinality) of a group.

- **Method**: `GET`
- **Path**: `/v1/group/{group}/cardinality`
- **URL Parameters**:
  - `group` (string): Unique identifier for the group.
- **Response Body**:
  ```json
  {
    "group": "sensor-01",
    "cardinality": "3"
  }
  ```

#### Example Request
```bash
curl http://localhost:8081/v1/group/sensor-01/cardinality
```

---

### 1.4 Prometheus Metrics
Exposes service monitoring metrics. Served by the dedicated metrics listener (`-metrics-port`, default `8081`), **not** by the public HTTP gateway — `GET /metrics` on the gateway port returns 404.

- **Method**: `GET`
- **Path**: `/metrics` (same listener also serves `/healthz` and `/readyz`)
- **Response Body**: Standard Prometheus plain-text metrics format.

#### Example Request
```bash
curl http://localhost:8181/metrics
```

---

### 1.5 ID Semantics

`id`/`ids` are opaque strings. On the write path every id is mapped to a `uint64` with murmur3-64:

```go
hashID(s) = murmur3.Sum64([]byte(s))
```

That `uint64` is what travels in the Raft payload and what the sketch stores, which gives clients three properties they must plan for:

| Property | Consequence |
|---|---|
| **One-way** | The original string is not recoverable from a sketch — only the count is. Keep your own index if you need to resolve ids |
| **Lossy (64-bit)** | Two ids that collide count once, an under-count of one — far inside HLL's own error budget, but exact for the `bitmap` algorithm |
| **Part of the on-disk format** | Changing the mapping invalidates persisted sketches, so it needs a migration or a fresh group — not a refactor |

Algorithm-specific consumption:

- `hll`: the `uint64` is hashed again in its **decimal** form (`murmur3.Sum64([]byte(strconv.FormatUint(id, 10)))`) to select the register. Two hash steps, both part of the format — see [theory.md](theory.md) §1.
- `bitmap`: the `uint64` is stored verbatim in a Roaring64 bitmap, so counts are exact for the mapped ids.

Fixed vectors, pinning the contract in CI (`TestHashIDContract` for the string → `uint64` step in `internal/server`, `TestAddHashCompat` for the decimal-form register derivation in `internal/cardinality/hll`):

| Input | `hashID` | decimal-form hash |
|---|---|---|
| `"user-123"` | `17358980885433774459` | `3061975231960053836` |
| `"user-456"` | `1244570398996739023` | `8857194335805247989` |
| `"a"` | `9607679276477937801` | `12943611033036980301` |

Empty ids are rejected with `InvalidArgument` before hashing.

---

## 2. gRPC API

The service is defined under package `cardinality.v1`.

### 2.1 Service Definition
```protobuf
service CardinalityService {
  rpc Add(AddRequest) returns (AddResponse);
  rpc BatchAdd(BatchAddRequest) returns (AddResponse);
  rpc Query(QueryRequest) returns (QueryResponse);
}
```

### 2.2 Message Payloads

#### `AddRequest`
```protobuf
message AddRequest {
  string group = 1;
  string id    = 2;
}
```
`id` is the same opaque string as in the REST API; the string → `uint64` mapping (§1.5) is fixed and persisted with the sketch.

#### `BatchAddRequest`
```protobuf
message BatchAddRequest {
  string          group = 1;
  repeated string ids   = 2;
}
```

#### `AddResponse`
```protobuf
message AddResponse {
  bool ok = 1;
}
```

#### `QueryRequest`
```protobuf
message QueryRequest {
  string group    = 1;
  bool   stale_ok = 2;
}
```

#### `QueryResponse`
```protobuf
message QueryResponse {
  string group       = 1;
  uint64 cardinality = 2;
}
```
