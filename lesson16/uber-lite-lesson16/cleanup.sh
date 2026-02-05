#!/bin/bash
# Stop lesson16 containers and remove unused Docker resources.
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "Stopping dashboard (if running)..."
pkill -f "dashboard_server.py" 2>/dev/null || true
sleep 1

echo "Stopping uber-lite-lesson16 containers..."
(docker compose down -v 2>/dev/null || docker-compose down -v 2>/dev/null) || true
docker ps -a --filter "name=uber-lite-lesson16" -q | xargs -r docker stop 2>/dev/null || true
docker ps -a --filter "name=uber-lite-lesson16" -q | xargs -r docker rm 2>/dev/null || true

echo "Removing unused Docker resources..."
docker network prune -f 2>/dev/null || true
docker volume prune -f 2>/dev/null || true

echo "Done. Containers stopped and unused resources removed."
