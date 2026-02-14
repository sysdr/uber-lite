#!/usr/bin/env bash
# Stop Lesson 24 services and remove unused Docker resources.
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "Stopping dashboard (if running)..."
pkill -f "dashboard_server.py" 2>/dev/null || true
sleep 1

echo "Stopping Lesson 24 Docker containers..."
(docker compose down -v 2>/dev/null || docker-compose down -v 2>/dev/null || true)

echo "Removing stopped containers..."
docker container prune -f 2>/dev/null || true

echo "Removing unused networks..."
docker network prune -f 2>/dev/null || true

echo "Cleanup complete."
