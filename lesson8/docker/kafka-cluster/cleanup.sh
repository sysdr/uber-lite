#!/bin/bash
# cleanup.sh - Stop Lesson 8 dashboard and Docker cluster, remove unused Docker resources

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "Stopping Lesson 8 dashboard..."
pkill -f "dashboard_server.py" 2>/dev/null || true

if [ -f "$SCRIPT_DIR/cluster-ops.sh" ]; then
  echo "Stopping Kafka cluster..."
  (cd "$SCRIPT_DIR" && ./cluster-ops.sh stop) || true
else
  echo "No cluster-ops.sh; stopping any lesson8-related containers..."
  ids=$(docker ps -q --filter "name=kafka-" --filter "name=zookeeper" --filter "name=schema-registry" 2>/dev/null) || true
  [ -n "$ids" ] && echo "$ids" | xargs docker stop 2>/dev/null || true
fi

echo "Removing unused Docker resources (containers, networks, images)..."
docker system prune -f

echo "Cleanup complete."
