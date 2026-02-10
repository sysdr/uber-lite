#!/bin/bash
# Stop lesson19 containers and remove unused Docker resources
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "Stopping dashboard..."
pkill -f "dashboard_server.py" 2>/dev/null || true
sleep 1

echo "Stopping lesson19 Docker stack..."
docker compose down 2>/dev/null || docker-compose down 2>/dev/null || true

echo "Stopping any lesson19 containers by name..."
docker ps -a --filter "name=lesson19" --format "{{.Names}}" 2>/dev/null | xargs -r docker stop 2>/dev/null || true
docker ps -a --filter "name=lesson19" --format "{{.Names}}" 2>/dev/null | xargs -r docker rm 2>/dev/null || true

echo "Removing unused Docker resources (containers, networks)..."
docker container prune -f 2>/dev/null || true
docker network prune -f 2>/dev/null || true

echo "Done. Optional: run 'docker image prune -f' to remove unused images, 'docker volume prune -f' to remove unused volumes."
