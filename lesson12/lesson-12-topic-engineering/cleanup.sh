#!/bin/bash
# Stop Lesson 12 dashboard, lesson9 Kafka containers, and remove unused Docker resources.
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
L9="$SCRIPT_DIR/../../lesson9/uber-lite"

echo "Stopping Lesson 12 dashboard (if running)..."
pkill -f "lesson-12-topic-engineering/dashboard_server.py" 2>/dev/null || true
pkill -f "dashboard_server.py" 2>/dev/null || true
sleep 1

echo "Stopping Docker containers (uber-lite Kafka cluster)..."
if [ -f "$L9/docker-compose.yml" ]; then
  (cd "$L9" && (docker compose down 2>/dev/null || docker-compose down 2>/dev/null)) || true
fi
docker ps -a --filter "name=uber-lite" -q 2>/dev/null | xargs -r docker stop 2>/dev/null || true
docker ps -a --filter "name=uber-lite" -q 2>/dev/null | xargs -r docker rm 2>/dev/null || true

echo "Removing unused Docker resources..."
docker network prune -f 2>/dev/null || true
docker volume prune -f 2>/dev/null || true

echo "Done. Containers stopped and unused resources removed."
