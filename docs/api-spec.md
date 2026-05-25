# API Specification

This document defines the REST and gRPC API specifications for the Time-Series Cardinality Tracker.

---

## 1. REST API (HTTP/JSON)

The HTTP REST API is exposed by the HTTP gateway. All request values (`value` or `values`) sent via JSON must be **Base64 encoded** byte slices.

### 1.1 Add Value
Adds a single unique item to a time-series HLL sketch.

- **Method**: `POST`
- **Path**: `/v1/series/{series_id}/add`
- **Headers**:
  - `Content-Type: application/json`
- **URL Parameters**:
  - `series_id` (string): Unique identifier for the time-series.
- **Request Body**:
  ```json
  {
    "value": "<base64_encoded_value>"
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
curl -X POST http://localhost:8081/v1/series/sensor-01/add \
  -H "Content-Type: application/json" \
  -d '{"value": "dXNlci0xMjM="}'
```

---

### 1.2 Batch Add Values
Adds multiple unique items to a time-series HLL sketch in a single request.

- **Method**: `POST`
- **Path**: `/v1/series/{series_id}/batch`
- **Headers**:
  - `Content-Type: application/json`
- **URL Parameters**:
  - `series_id` (string): Unique identifier for the time-series.
- **Request Body**:
  ```json
  {
    "values": [
      "<base64_encoded_value_1>",
      "<base64_encoded_value_2>"
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
curl -X POST http://localhost:8081/v1/series/sensor-01/batch \
  -H "Content-Type: application/json" \
  -d '{"values": ["dXNlci00NTY=", "dXNlci03ODk="]}'
```

---

### 1.3 Query Cardinality
Retrieves the estimated unique item count (cardinality) of a time-series.

- **Method**: `GET`
- **Path**: `/v1/series/{series_id}/cardinality`
- **URL Parameters**:
  - `series_id` (string): Unique identifier for the time-series.
- **Response Body**:
  ```json
  {
    "seriesId": "sensor-01",
    "cardinality": "3"
  }
  ```

#### Example Request
```bash
curl http://localhost:8081/v1/series/sensor-01/cardinality
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
  string series_id = 1;
  bytes  value     = 2;
}
```

#### `BatchAddRequest`
```protobuf
message BatchAddRequest {
  string         series_id = 1;
  repeated bytes values    = 2;
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
  string series_id = 1;
  bool   stale_ok  = 2;
}
```

#### `QueryResponse`
```protobuf
message QueryResponse {
  string series_id   = 1;
  uint64 cardinality = 2;
}
```
