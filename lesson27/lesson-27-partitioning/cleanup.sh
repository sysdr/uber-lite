#!/usr/bin/env bash
# Stop lesson-27 Docker containers and remove unused Docker resources.
set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"

echo "==> Stopping application (PartitionedStreamsApp)..."
pkill -f "PartitionedStreamsApp" 2>/dev/null || true
pkill -f "exec:java.*lesson27" 2>/dev/null || true

echo "==> Stopping Docker Compose (Kafka + Zookeeper)..."
docker compose down -v 2>/dev/null || true

echo "==> Removing unused Docker resources..."
docker system prune -f
docker volume prune -f 2>/dev/null || true
docker network prune -f 2>/dev/null || true

echo "==> Cleanup done."
