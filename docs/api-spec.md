# API Specification

This document defines the REST and gRPC API specifications for the Group Cardinality Tracker.

---

## 1. REST API (HTTP/JSON)

The HTTP REST API is exposed by the HTTP gateway. All request values (`id` or `ids`) sent via JSON are plain strings.

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
Exposes service monitoring metrics.

- **Method**: `GET`
- **Path**: `/metrics`
- **Response Body**: Standard Prometheus plain-text metrics format.

#### Example Request
```bash
curl http://localhost:8081/metrics
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
