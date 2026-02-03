#!/bin/bash
# Stop Lesson 13 dashboard, Kafka containers, and remove unused Docker resources.
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "Stopping Lesson 13 dashboard (if running)..."
pkill -f "kafka-producer-lesson.*dashboard_server.py" 2>/dev/null || true
pkill -f "lesson13.*dashboard_server.py" 2>/dev/null || true
pkill -f "dashboard_server.py" 2>/dev/null || true
sleep 1

echo "Stopping Docker containers (lesson13 Kafka cluster)..."
if [ -f "$SCRIPT_DIR/docker-compose.yml" ]; then
  (cd "$SCRIPT_DIR" && (docker compose down 2>/dev/null || docker-compose down 2>/dev/null)) || true
fi
docker ps -a --filter "name=lesson13" -q 2>/dev/null | xargs -r docker stop 2>/dev/null || true
docker ps -a --filter "name=lesson13" -q 2>/dev/null | xargs -r docker rm 2>/dev/null || true
docker ps -a --filter "name=kafka-producer-lesson" -q 2>/dev/null | xargs -r docker stop 2>/dev/null || true
docker ps -a --filter "name=kafka-producer-lesson" -q 2>/dev/null | xargs -r docker rm 2>/dev/null || true

echo "Removing unused Docker resources..."
docker network prune -f 2>/dev/null || true
docker volume prune -f 2>/dev/null || true

echo "Done. Containers stopped and unused resources removed."
