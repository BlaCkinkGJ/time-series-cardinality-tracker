# Deployment Guide

This document covers deploying the Group Cardinality Tracker in **Docker Compose** (local/dev) and **Kubernetes** (production).

---

## Directory Layout

```
deploy/
├── docker/
│   ├── Dockerfile            # Multi-stage CGO_ENABLED=0 build
│   └── docker-compose.yml    # 3-node local cluster
└── kubernetes/
    ├── namespace.yaml        # cardinality namespace
    ├── statefulset.yaml      # 3-replica StatefulSet with per-pod PVC
    ├── service-headless.yaml # Headless service for stable pod DNS
    ├── service-lb.yaml       # LoadBalancer for external access
    └── servicemonitor.yaml   # Prometheus ServiceMonitor (optional)
```

---

## 1. Docker Compose (Local / Dev)

### Prerequisites
- Docker ≥ 24 with Compose V2

### Quick Start

```bash
cd deploy/docker
docker compose build
docker compose up -d
```

### Ports

| Node   | HTTP (REST + metrics) | gRPC  |
|--------|-----------------------|-------|
| node1  | `8081`                | `9091`|
| node2  | `8082`                | `9092`|
| node3  | `8083`                | `9093`|

### Verify

```bash
# Add an ID to group "prod" via node1
curl -X POST http://localhost:8081/v1/group/prod/add \
  -H "Content-Type: application/json" \
  -d '{"id": "dXNlci0xMjM="}'

# Query cardinality from any node (forwarded automatically)
curl http://localhost:8082/v1/group/prod/cardinality
```

### Teardown

```bash
cd deploy/docker
docker compose down -v
```

---

## 2. Kubernetes

### Prerequisites
- Kubernetes ≥ 1.21
- `kubectl` configured
- A container registry accessible from the cluster

### 2.1 Build and Push Image

```bash
# Build
docker build -f deploy/docker/Dockerfile -t <your-registry>/cardinality-tracker:latest .

# Push
docker push <your-registry>/cardinality-tracker:latest
```

Update the `image:` field in [statefulset.yaml](../deploy/kubernetes/statefulset.yaml) to your pushed image.

### 2.2 Apply Manifests

```bash
# Create namespace first
kubectl apply -f deploy/kubernetes/namespace.yaml

# Apply all remaining manifests
kubectl apply -f deploy/kubernetes/
```

### 2.3 Verify Rollout

```bash
kubectl -n cardinality rollout status statefulset/cardinality
kubectl -n cardinality get pods
```

Expected output (3 pods Running):
```
NAME            READY   STATUS    RESTARTS   AGE
cardinality-0   1/1     Running   0          30s
cardinality-1   1/1     Running   0          25s
cardinality-2   1/1     Running   0          20s
```

### 2.4 Access the Service

```bash
# Get LoadBalancer external IP
kubectl -n cardinality get svc cardinality-lb

# Once EXTERNAL-IP is assigned:
export LB_IP=$(kubectl -n cardinality get svc cardinality-lb -o jsonpath='{.status.loadBalancer.ingress[0].ip}')

# Add an ID
curl -X POST http://$LB_IP:8080/v1/group/prod/add \
  -H "Content-Type: application/json" \
  -d '{"id": "dXNlci0xMjM="}'

# Query cardinality
curl http://$LB_IP:8080/v1/group/prod/cardinality
```

> **Note:** The LoadBalancer round-robins across all 3 nodes. Each node uses the consistent-hash ring to forward requests to the correct shard owner transparently.

### 2.5 Pod DNS (Internal Shard Routing)

Each pod is reachable at a stable DNS name via the headless service:

```
cardinality-0.cardinality.cardinality.svc.cluster.local:9090
cardinality-1.cardinality.cardinality.svc.cluster.local:9090
cardinality-2.cardinality.cardinality.svc.cluster.local:9090
```

The `-peers` flag is pre-configured with these addresses in `statefulset.yaml`.

### 2.6 Node ID Derivation

The StatefulSet uses a shell wrapper to derive `-node-id` from the pod hostname:

```sh
exec /server -node-id=$((${HOSTNAME##*-}+1)) ...
```

| Pod hostname    | `${HOSTNAME##*-}` | `node-id` |
|-----------------|-------------------|-----------|
| `cardinality-0` | `0`               | `1`       |
| `cardinality-1` | `1`               | `2`       |
| `cardinality-2` | `2`               | `3`       |

### 2.7 Scaling

To add nodes, update both `replicas` in `statefulset.yaml` and the `-peers` flag to include the new pod DNS names, then re-apply.

### 2.8 Prometheus Monitoring (Optional)

If you have the Prometheus Operator installed:

```bash
kubectl apply -f deploy/kubernetes/servicemonitor.yaml
```

This creates a `ServiceMonitor` scraping `/metrics` on port `8080` every 15s.

Without the Prometheus Operator, add a standard Prometheus scrape config:

```yaml
scrape_configs:
  - job_name: cardinality-tracker
    kubernetes_sd_configs:
      - role: pod
        namespaces:
          names: [cardinality]
    relabel_configs:
      - source_labels: [__meta_kubernetes_pod_label_app]
        regex: cardinality
        action: keep
      - source_labels: [__meta_kubernetes_pod_ip]
        target_label: __address__
        replacement: '$1:8080'
```

### 2.9 Teardown

```bash
kubectl delete namespace cardinality
```

> **Warning:** This deletes all PersistentVolumeClaims and their data.

---

## 3. Server Flags Reference

| Flag         | Default                  | Description                                 |
|--------------|--------------------------|---------------------------------------------|
| `-grpc-port` | `9090`                   | gRPC listen port                            |
| `-http-port` | `8080`                   | HTTP REST gateway + `/metrics` listen port  |
| `-data`      | `/tmp/cardinality-data`  | BadgerDB data directory (use a PVC in prod) |
| `-node-id`   | `1`                      | Raft node ID (must be unique per node, ≥1)  |
| `-peers`     | `""`                     | Comma-separated `host:port` of all nodes    |
