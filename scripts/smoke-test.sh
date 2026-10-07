#!/bin/bash
set -euo pipefail

echo "==> Starting 3-node cardinality tracker cluster..."
cd "$(dirname "$0")/../deploy/docker"
# --build: up alone reuses an existing image, which would smoke-test stale
# source. Layer cache keeps the rebuild cheap.
docker compose up -d --build

# Cleanup on exit (in case of failure)
trap 'echo "==> Cleaning up cluster..."; docker compose down -v' EXIT

echo "==> Waiting for Raft leader elections..."
sleep 5

echo "==> Checking container status..."
docker compose ps

# Verify initial cardinality is 0
echo "==> Querying initial cardinality from node 1..."
INIT_CARD=$(curl -s http://localhost:8081/v1/group/test/cardinality | jq -r '.cardinality')
if [ "$INIT_CARD" != "0" ]; then
  echo "Expected initial cardinality 0, got $INIT_CARD"
  docker compose logs
  exit 1
fi
echo "Initial cardinality verified: 0"

# Add 100 IDs to node 1
echo "==> Adding 100 items via node 1 HTTP gateway..."
for i in $(seq 1 100); do
  val="user-$i"
  curl -s -X POST http://localhost:8081/v1/group/prod/add -d "{\"id\":\"$val\"}" > /dev/null
done

# Query cardinality from node 2 (should forward to the owner node)
echo "==> Querying cardinality from node 2..."
FINAL_CARD=$(curl -s http://localhost:8082/v1/group/prod/cardinality | jq -r '.cardinality')

echo "==> Querying cardinality from node 3..."
FINAL_CARD_NODE3=$(curl -s http://localhost:8083/v1/group/prod/cardinality | jq -r '.cardinality')

echo "Node 2 reported cardinality: $FINAL_CARD"
echo "Node 3 reported cardinality: $FINAL_CARD_NODE3"

# Verify estimate is close to 100 (HLL has <3% error, so 95-105 is valid)
if [ "$FINAL_CARD" -lt 95 ] || [ "$FINAL_CARD" -gt 105 ]; then
  echo "Error: Cardinality estimate $FINAL_CARD is out of range [95, 105]"
  docker compose logs
  exit 1
fi

echo "==> Smoke test PASSED successfully!"

# Observability endpoints: /metrics, /healthz and /readyz live on the
# dedicated metrics port (host 818x -> container 8081), never on the
# public gateway port (808x).
echo "==> Verifying observability endpoints..."
if ! curl -sf http://localhost:8181/healthz > /dev/null; then
  echo "Error: metrics port 8181 is unreachable"
  docker compose ps
  docker compose logs node1
  exit 1
fi

for port in 8181 8182 8183; do
  HEALTH=$(curl -s -o /dev/null -w '%{http_code}' "http://localhost:$port/healthz")
  READY=$(curl -s -o /dev/null -w '%{http_code}' "http://localhost:$port/readyz")
  if [ "$HEALTH" != "200" ] || [ "$READY" != "200" ]; then
    echo "Error: node on $port reported /healthz=$HEALTH /readyz=$READY, want 200/200"
    docker compose logs
    exit 1
  fi
done
echo "All nodes report /healthz and /readyz 200"

# Only the shard owner holds the group, so sum the per-group gauge across
# every node instead of assuming a node. `|| true` keeps a curl failure
# from killing the script silently under set -e; an empty value then
# fails the range check below.
GROUP_CARD=0
for port in 8181 8182 8183; do
  VALUE=$(curl -s "http://localhost:$port/metrics" |
    awk -F' ' '/^cardinality_tracker_group_cardinality\{group="prod"\}/ { print $2 }' || true)
  if [ -n "$VALUE" ]; then
    GROUP_CARD=$(awk -v a="$GROUP_CARD" -v b="$VALUE" 'BEGIN { print a + b }')
  fi
done
echo "Summed group_cardinality{group=\"prod\"} across nodes: $GROUP_CARD"

if ! awk -v v="$GROUP_CARD" 'BEGIN { exit !(v >= 95 && v <= 105) }'; then
  echo "Error: exported cardinality $GROUP_CARD is out of range [95, 105]"
  docker compose logs
  exit 1
fi

LEADER=$(curl -s http://localhost:8181/metrics |
  awk '/^cardinality_tracker_raft_is_leader/ { print $2 }' || true)
if [ "$LEADER" != "1" ]; then
  echo "Error: raft_is_leader on node 1 is '$LEADER', want 1"
  docker compose logs
  exit 1
fi
echo "Raft leadership reported: $LEADER"

GATEWAY_METRICS=$(curl -s -o /dev/null -w '%{http_code}' http://localhost:8081/metrics)
if [ "$GATEWAY_METRICS" = "200" ]; then
  echo "Error: /metrics is still served on the public gateway port 8081"
  exit 1
fi
echo "==> Observability endpoints PASSED (public gateway returns $GATEWAY_METRICS for /metrics)"

# Restart every node: the Raft log is in-memory, so this only works if the
# startup path restores sketches from Badger (store.LoadAll -> Engine.Restore).
echo "==> Restarting all nodes to verify restart recovery..."
docker compose restart > /dev/null
sleep 5

RESTART_CARD=$(curl -s http://localhost:8081/v1/group/prod/cardinality | jq -r '.cardinality')
echo "Node 1 reported cardinality after restart: $RESTART_CARD"

if [ "$RESTART_CARD" -lt 95 ] || [ "$RESTART_CARD" -gt 105 ]; then
  echo "Error: cardinality $RESTART_CARD did not survive the restart"
  docker compose logs
  exit 1
fi
echo "==> Restart recovery PASSED: cardinality $RESTART_CARD survived"
