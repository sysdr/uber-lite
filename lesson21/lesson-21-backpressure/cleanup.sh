#!/bin/bash
# Stop dashboard, lesson-21 containers, and remove unused Docker resources.
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "Stopping dashboard (if running)..."
pkill -f "lesson-21-backpressure.*dashboard_server.py" 2>/dev/null || true
pkill -f "lesson21.*dashboard_server.py" 2>/dev/null || true
pkill -f "dashboard_server.py" 2>/dev/null || true
sleep 2

echo "Stopping lesson-21-backpressure containers..."
docker compose down -v 2>/dev/null || docker-compose down -v 2>/dev/null || true
docker ps -a --filter "name=lesson-21-backpressure" -q 2>/dev/null | xargs -r docker stop 2>/dev/null || true
docker ps -a --filter "name=lesson-21-backpressure" -q 2>/dev/null | xargs -r docker rm 2>/dev/null || true

echo "Removing unused Docker resources..."
docker network prune -f 2>/dev/null || true
docker volume prune -f 2>/dev/null || true

echo "Done. Dashboard and containers stopped; unused networks/volumes removed."
