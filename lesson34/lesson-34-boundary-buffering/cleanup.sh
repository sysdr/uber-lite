#!/usr/bin/env bash
# Stop lesson34 Docker containers and remove unused Docker resources for this project.
set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"

echo "==> Stopping dashboard and app processes..."
pkill -f "com.uberlite.boundary.MetricsDashboardApp" 2>/dev/null || true
pkill -f "com.uberlite.boundary.BoundaryBufferingApp" 2>/dev/null || true
sleep 2

echo "==> Stopping Docker Compose (lesson-34-boundary-buffering)..."
docker compose down -v --remove-orphans 2>/dev/null || true

echo "==> Removing project containers (if any remain)..."
docker rm -f lesson-34-boundary-buffering-zookeeper-1 lesson-34-boundary-buffering-kafka-1 lesson-34-boundary-buffering-kafka-init-1 2>/dev/null || true

echo "==> Removing unused Docker resources (optional)..."
docker network prune -f 2>/dev/null || true
docker volume prune -f 2>/dev/null || true

echo "==> Cleanup done. Containers and app processes stopped."
