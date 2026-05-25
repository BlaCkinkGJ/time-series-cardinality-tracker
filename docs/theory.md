# Math and Architectural Tradeoffs Explanation

This document explains the mathematical foundations, core algorithms, and structural tradeoffs chosen for the Distributed Cardinality Tracker.

---

## 1. HyperLogLog++: Mathematics of Cardinality Estimation

HyperLogLog (HLL) is an algorithm designed to estimate the number of unique elements (cardinality) in a stream of data using minimal memory.

### 1.1 How HLL Works (Stochastic Averaging)
1. Every input item is hashed into a 64-bit value using a uniform hash function.
2. The first $p$ bits of the hash are used to determine which bucket (register) the item belongs to. We have $m = 2^p$ registers. For $p=14$, we use $2^{14} = 16,384$ registers.
3. The remaining $64-p$ bits are analyzed to find the index of the first `1` bit (from the left). Let this index be $\rho$.
4. The register stores the maximum value of $\rho$ seen so far for that bucket:
   $$M[j] = \max(M[j], \rho)$$

### 1.2 The Estimate Formula
The raw estimate (indicator) is calculated using the harmonic mean of the registers:
$$E = \alpha_m \cdot m^2 \cdot \left( \sum_{j=1}^m 2^{-M[j]} \right)^{-1}$$

Where $\alpha_m$ is a correction constant:
$$\alpha_m = \frac{1}{2 \ln(2) \cdot (1 + \frac{1.079}{m})}$$

### 1.3 HyperLogLog++ Enhancements
Our engine incorporates the core advancements from the Google HyperLogLog++ paper:
- **64-bit Hash**: Solves hash collision limits present in original 32-bit HLL.
- **Bias Correction**: HLL has a small bias towards overestimation or underestimation at certain ranges. HLL++ applies empirical bias correction tables for small and medium ranges.
- **Linear Counting**: For small cardinalities (where many registers are still 0), HLL++ falls back to Linear Counting, which is more accurate:
  $$E_{lc} = m \ln( \frac{m}{V} )$$
  where $V$ is the number of registers containing 0.

### 1.4 Memory Profile
Because we use a fixed precision of $p=14$, each register requires only 1 byte (since the maximum run of zeros in a 64-bit hash can easily fit in 8 bits).
$$\text{Memory usage per series} = 16,384 \text{ bytes} \approx 16 \text{ KB}$$
This allows a single server to track **100,000 distinct metrics** using only **~1.6 GB** of memory!

---

## 2. Consistent Hashing: Even Distribution

Consistent hashing solves the problem of node membership changes (scaling up/down) in a distributed database.

### 2.1 Virtual Nodes
If we map nodes directly onto a hash ring, hash collisions or uneven spacing can lead to massive imbalance (hotspots).
To solve this, we map **150 virtual nodes** per physical node.
1. Physical address: `node1:9090`
2. Virtual addresses: `node1:9090#0`, `node1:9090#1`, ... `node1:9090#149`
3. Each virtual address is hashed to a uint32 point on the circle $[0, 2^{32}-1]$.

When a write arrives:
1. `series_id` is hashed to a uint32 point.
2. We traverse the ring clockwise to find the first virtual node hash that is $\ge$ the key's hash.
3. The request is routed to the physical node matching that virtual node.

### 2.2 Rebalancing Impact
- In a traditional hash modulo ring ($N \pmod M$), adding or removing a node requires remapping **almost all** keys.
- Under consistent hashing, adding/removing a node only requires moving **$1/N$** of the keys, significantly minimizing replication sync costs during cluster resizing.

---

## 3. Tradeoffs & Design Decisions

### 3.1 Single-Node Raft Groups vs. Shared Consensus Ring
- **The Problem**: A single global Raft cluster replicates all commands to all nodes. This severely limits horizontal write scalability since every node must process every write.
- **The Solution**: We shard the keyspace using Consistent Hashing first. Each node runs its own isolated single-node Raft group.
- **The Tradeoff**: If a node fails, its subset of keys is offline until it reboots (high availability is traded for maximum write throughput). Since BadgerDB persisted files are bound to the host volume, when the container restarts, it recovers its full FSM state instantly via the Raft snapshot and WAL logs.

### 3.2 LSM-Tree (BadgerDB) vs. B-Tree
- **The Problem**: Writing updated 16 KB register arrays constantly creates high random-write IOPS overhead.
- **The Solution**: BadgerDB uses a Log-Structured Merge-tree (LSM) architecture. Updates are written sequentially to an append-only log (SSTables) and consolidated in memory (MemTable).
- **The Tradeoff**: Read latency is slightly higher than B-Tree due to multiple levels, but write throughput is maximized. Since reads are served directly from the in-memory HLL Engine, read latency from disk is practically bypassed, optimizing for extreme performance.
