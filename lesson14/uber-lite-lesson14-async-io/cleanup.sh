#!/bin/bash
# Stop Lesson 14 containers and remove unused Docker resources.
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "Cleaning up Lesson 14..."
echo ""

# Stop dashboard
echo "Stopping dashboard..."
pkill -f "lesson14.*dashboard_server.py" 2>/dev/null || true
pkill -f "uber-lite-lesson14-async-io.*dashboard_server.py" 2>/dev/null || true
sleep 1
echo "  Dashboard stopped (if it was running)."

# Stop Docker containers (lesson14 cluster)
echo ""
echo "Stopping Docker containers..."
if [ -f "$SCRIPT_DIR/docker-compose.yml" ]; then
  (cd "$SCRIPT_DIR" && (docker compose down 2>/dev/null || docker-compose down 2>/dev/null)) || true
  docker ps -a --filter "name=lesson14-" -q | xargs -r docker stop 2>/dev/null || true
  docker ps -a --filter "name=lesson14-" -q | xargs -r docker rm 2>/dev/null || true
  echo "  Lesson 14 containers stopped."
else
  echo "  No docker-compose.yml found."
fi

# Remove unused Docker resources
echo ""
echo "Removing unused Docker resources..."
docker container prune -f 2>/dev/null || true
docker network prune -f 2>/dev/null || true
docker volume prune -f 2>/dev/null || true
echo "  Unused containers, networks, and volumes pruned."

# Remove target (build artifacts)
echo ""
echo "Removing build artifacts (target)..."
rm -rf "$SCRIPT_DIR/target" 2>/dev/null || true
echo "  target/ removed."

echo ""
echo "✅ Cleanup completed."
echo ""
echo "Summary:"
echo "  - Dashboard process stopped"
echo "  - Docker containers stopped (Lesson 14 cluster)"
echo "  - Unused Docker resources pruned"
echo "  - target/ directory removed"
