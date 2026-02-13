#!/bin/bash
# Stop all services and Docker; remove unused Docker resources.
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "Stopping Lesson 23 dashboard (if running)..."
pkill -f "dashboard_server.py" 2>/dev/null || true
sleep 1

echo "Stopping Docker containers..."
docker ps -q 2>/dev/null | xargs -r docker stop 2>/dev/null || true

echo "Stopping any lesson / uber-lite compose stacks..."
for dir in "$SCRIPT_DIR" "$SCRIPT_DIR/../../lesson9/uber-lite"; do
  if [ -d "$dir" ]; then
    (cd "$dir" && (docker compose down 2>/dev/null || docker-compose down 2>/dev/null)) || true
  fi
done
docker ps -a --filter "name=uber-lite" -q 2>/dev/null | xargs -r docker stop 2>/dev/null || true
docker ps -a --filter "name=lesson22" -q 2>/dev/null | xargs -r docker stop 2>/dev/null || true

echo "Removing unused Docker resources..."
docker network prune -f 2>/dev/null || true
docker volume prune -f 2>/dev/null || true
docker image prune -f 2>/dev/null || true

echo "Done. All services and Docker containers stopped; unused resources removed."
