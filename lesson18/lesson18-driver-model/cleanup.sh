#!/bin/bash
# Stop Lesson 18 dashboard, Docker containers, and remove unused Docker resources.
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "Stopping dashboard (if running)..."
pkill -f "lesson18.*dashboard_server.py" 2>/dev/null || true
pkill -f "lesson18-driver-model.*dashboard_server.py" 2>/dev/null || true
pkill -f "dashboard_server.py" 2>/dev/null || true
sleep 1

echo "Stopping Docker containers..."
docker compose down -v 2>/dev/null || docker-compose down -v 2>/dev/null || true
docker ps -a --filter "name=lesson18-driver-model" -q | xargs -r docker stop 2>/dev/null || true
docker ps -a --filter "name=lesson18-driver-model" -q | xargs -r docker rm 2>/dev/null || true

echo "Removing unused Docker resources..."
docker network prune -f 2>/dev/null || true
docker volume prune -f 2>/dev/null || true

echo "✓ Cleanup complete. Containers stopped and unused resources removed."
