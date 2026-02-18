#!/usr/bin/env bash
# Stop Lesson 26 services and remove unused Docker resources.
set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"

echo "==> Stopping LocalityMatchingApp (if running)..."
pkill -f LocalityMatchingApp 2>/dev/null || true
pkill -f "exec:java.*lesson-26-data-locality" 2>/dev/null || true
sleep 1

echo "==> Stopping Lesson 26 Docker containers..."
if [ -d "$DIR/lesson-26-data-locality" ]; then
  cd "$DIR/lesson-26-data-locality"
  docker compose down 2>/dev/null || docker-compose down 2>/dev/null || true
  cd "$DIR"
fi

# Stop/remove any lesson26 containers by name (in case compose wasn't used)
docker ps -a --filter "name=lesson26" -q | xargs -r docker stop 2>/dev/null || true
docker ps -a --filter "name=lesson26" -q | xargs -r docker rm 2>/dev/null || true

echo "==> Removing unused Docker resources..."
docker system prune -f
docker volume prune -f 2>/dev/null || true
docker network prune -f 2>/dev/null || true

echo "==> Cleanup done."
