# API Specification

This document defines the REST and gRPC API specifications for the Group Cardinality Tracker.

---

## 1. REST API (HTTP/JSON)

The HTTP REST API is exposed by the HTTP gateway. Request values (`id` or `ids`) are 64-bit integers; how they are interpreted is fixed, not an implementation detail: see [§1.5 ID Semantics](#15-id-semantics).

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
    "id": 12345
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
  -d '{"id": 12345}'
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
      12345,
      67890
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
  -d '{"ids": [12345, 67890]}'
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

`id`/`ids` are the caller's 64-bit identifiers (`uint64`), not strings. The server stores the value as given — no hashing at the API layer — and the algorithm decides what to do with it:

- `hll` hashes the id's 8 little-endian bytes (one murmur3 pass) to pick a register, so the distribution is uniform however you number your ids.
- `bitmap` stores the `uint64` verbatim in a Roaring64 bitmap, so its counts are **exact over your id space**.

What that means for clients:

| Property | Consequence |
|---|---|
| **You own the mapping** | Every writer of a group must map a logical id to the same `uint64`; the server cannot detect a disagreement — it just counts two values |
| **Opaque values** | `0` is a valid id, and the original id is not recoverable from a sketch — only the count is |
| **Format-bound** | The derivation is part of the persisted sketch format, so changing it re-hashes every stored group: a migration, not a refactor |

JSON encoding follows proto3: 64-bit integers are accepted as numbers or strings (`{"id": 12345}` and `{"id": "12345"}` are the same id), and `cardinality` comes back as a string.

One consequence of proto3: `uint64` has no field presence, so an **omitted `id` is indistinguishable from `id: 0`** — always send the field. A non-numeric value is rejected with `InvalidArgument`.

```bash
curl -X POST http://localhost:8081/v1/group/sensor-01/add \
  -H "Content-Type: application/json" \
  -d '{"id": 12345}'
```

---

### 1.6 Merge Sketch
Unions a serialised sketch — produced by another node or cluster — into a group. Use it to aggregate two trackers that saw different ids for the same logical group.

- **Method**: `POST`
- **Path**: `/v1/group/{group}/merge`
- **Headers**:
  - `Content-Type: application/json`
- **URL Parameters**:
  - `group` (string): Unique identifier for the group.
- **Request Body**:
  ```json
  {
    "algo": "hll",
    "sketch": "<base64 serialised sketch>"
  }
  ```
- **Response Body**:
  ```json
  {
    "ok": true
  }
  ```

`algo` is the algorithm key (`hll`, `bitmap`) that produced the bytes; the engine rejects a mismatch (`InvalidArgument`) instead of corrupting the group. `sketch` is the algorithm's own serialisation — for `hll`, the 2-byte precision prefix followed by the register array. Merging is a union, so applying the same sketch twice changes nothing, and it is safe to replay after a retry.

The merge is state, not a suggestion: with Raft configured it becomes a `MERGE_SKETCH` entry, is applied on every replica and persisted, so it survives a restart.

#### Example Request
```bash
# sketch.b64 holds the base64 output of the other tracker's sketch
curl -X POST http://localhost:8081/v1/group/sensor-01/merge \
  -H "Content-Type: application/json" \
  -d "{\"algo\": \"hll\", \"sketch\": \"$(cat sketch.b64)\"}"
```

---

## 2. gRPC API

The service is defined under package `cardinality.v1`.

### 2.1 Service Definition
```protobuf
service CardinalityService {
  rpc Add(AddRequest) returns (AddResponse);
  rpc BatchAdd(BatchAddRequest) returns (AddResponse);
  rpc Query(QueryRequest) returns (QueryResponse);
  rpc Merge(MergeRequest) returns (MergeResponse);
}
```

### 2.2 Message Payloads

#### `AddRequest`
```protobuf
message AddRequest {
  string group = 1;
  uint64 id    = 2;
}
```

#### `BatchAddRequest`
```protobuf
message BatchAddRequest {
  string          group = 1;
  repeated uint64 ids   = 2;
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

#### `MergeRequest`
```protobuf
message MergeRequest {
  string group  = 1;
  string algo   = 2;  // "hll", "bitmap"
  bytes  sketch = 3;  // serialised sketch from that algorithm
}
```

#### `MergeResponse`
```protobuf
message MergeResponse {
  bool ok = 1;
}
```
