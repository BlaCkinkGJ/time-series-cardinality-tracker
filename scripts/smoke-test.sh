#!/bin/bash
set -euo pipefail

echo "==> Starting 3-node cardinality tracker cluster..."
cd "$(dirname "$0")/../deploy/docker"
docker compose up -d

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
