# Architectural Design Document

This document outlines the detailed system design and architectural components of the Distributed Group Cardinality Tracker.

---

## 1. System Topology

The tracker operates as a decentralized, shared-nothing cluster of nodes. The routing and storage responsibility of each key is deterministic and governed by a consistent hashing ring.

```
                  +-----------------------------------+
                  |        Client Request             |
                  |     (Add/Query group "my-group")  |
                  +-----------------+-----------------+
                                    |
                                    v
                       +------------+------------+
                       |      gRPC Server        |
                       |       (Node 1)          |
                       +------------+------------+
                                    |
                        [Resolve Owner of "my-group"]
                                    |
                  +------------------+------------------+
                  | (Resolves to Node 2)                | (Resolves to Node 1)
                  v                                     v
      +-----------+-----------+             +-----------+-----------+
      |      gRPC Client      |             |     Local Engine      |
      |   (Forward to Node 2) |             |  - Update HLL Sketch  |
      +-----------+-----------+             |  - Propose to Raft    |
                  |                         |  - Commit to Badger   |
           (gRPC Network)                   +-----------------------+
                  |
                  v
      +-----------+-----------+
      |      gRPC Server      |
      |       (Node 2)        |
      +-----------+-----------+
                  |
         [Process Locally]
                  v
      +-----------------------+
      |     Local Engine      |
      +-----------------------+
```

---

## 2. Component Design

### 2.1 Shard Router (`internal/router`)
To ensure even distribution and prevent hotspotting, we implement a **Virtual-Node Consistent Hash Ring**:
- **Hash Function**: 32-bit Murmur3 (`github.com/spaolacci/murmur3`), which provides extremely uniform keyspace distribution and low execution latency.
- **Virtual Nodes**: 150 virtual nodes per physical host. Virtual nodes are hashed using `addr#vnode_index` (e.g. `node1:9090#0`, `node1:9090#1`).
- **Binary Search Lookup**: Sorted slice of hash points is queried using `sort.Search` to map a group hash to the closest clockwise virtual node.

### 2.2 gRPC & Proxy Layer (`internal/server`)
Each node exposes both a REST gateway (`grpc-gateway`) and a gRPC server:
- Nodes hold a thread-safe connection pool of clients (`map[string]*grpc.ClientConn`) for other peers.
- When an API request comes in, the server evaluates `owner := ring.Resolve(group)`.
- If `owner != selfAddr`, it forwards the request over the cached gRPC connection to the owner. This makes sharding completely transparent to external clients.

### 2.3 Raft Durability Layer (`internal/raft`)
Although nodes run independently from a sharding perspective, each **shard** utilizes a Raft group (`go.etcd.io/etcd/raft/v3`) for its own local sharded store:
- **WAL Durability**: Write proposals are processed sequentially through the Raft log.
- **FSM Application**: Once committed by Raft, entries are applied to the in-memory HLL engine and written to BadgerDB.
- **Log Compaction & Snapshots**: Every 10,000 log entries, the FSM state (serialised maps of HLL sketch registers) is dumped as a Raft snapshot. The memory storage is then compacted, discarding old log entries.

### 2.4 HyperLogLog++ Engine (`internal/hll`)
Cardinality estimation is powered by a HyperLogLog++ implementation:
- **Precision**: $p=14$ (giving $2^{14} = 16,384$ registers). This guarantees a standard error of $\le 1.04/\sqrt{m} \approx 0.81\%$, which comfortably complies with the user requirement of $<3\%$.
- **Storage Profile**: Fixed-size byte slices of $16,384$ bytes represent the register states.
- **Thread Safety**: The `Engine` struct wraps a standard map (`map[string]*HLL`) with a RWMutex to allow fast concurrent reads and lock-guaranteed writes.

### 2.5 Storage Layer (`internal/store`)
- **Engine**: BadgerDB v4, a fast Log-Structured Merge (LSM) key-value database written in Go.
- **Key Layout**: Binary keys prefixing the group (`hll/<group>`).
- **Disk Sync**: Writes bypass CGO to avoid native dyld errors on macOS, providing a zero-dependency static build.

---

## 3. Data Flow Execution

### 3.1 Write path (Add Request)
1. Request arrives at `node_X`.
2. `node_X` hashes `group` and finds the owning node `node_Y`.
3. If `node_X == node_Y`:
   - Proposes `Command_ADD` containing ID to the local Raft group.
   - Raft appends log and advances state machine.
   - State machine updates in-memory HLL sketch registers.
   - Updated sketch registers are committed synchronously to BadgerDB.
4. If `node_X != node_Y`:
   - `node_X` forwards request to `node_Y` via gRPC.
   - `node_Y` runs step (3) and responds to `node_X`.

### 3.2 Read path (Query Request)
1. Query arrives at `node_X`.
2. `node_X` hashes `group` to find owner `node_Y`.
3. If `node_X == node_Y`:
   - Node queries the local `hll.Engine` map in-memory.
   - Returns the estimated cardinality estimate instantly.
4. If `node_X != node_Y`:
   - `node_X` queries `node_Y` via gRPC.
   - Returns the result back to client.
