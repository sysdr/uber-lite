#!/usr/bin/env bash
# Lesson 10 cleanup: stop dashboard, stop Docker containers, remove unused Docker resources.
# Run from uber-lite-lesson10 (project) directory.

set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "Stopping Lesson 10 dashboard (if running)..."
pkill -f "dashboard_server.py" 2>/dev/null || true
pkill -f "uber-lite-lesson10/dashboard_server.py" 2>/dev/null || true
sleep 1

echo "Stopping Docker Compose (Lesson 9 stack, if running)..."
LESSON10_DIR="$(dirname "$SCRIPT_DIR")"
COMPOSE_DIR="${LESSON10_DIR}/../lesson9/uber-lite"
if [ -f "${COMPOSE_DIR}/docker-compose.yml" ]; then
  (cd "${COMPOSE_DIR}" && docker compose down 2>/dev/null) || true
else
  echo "  No docker-compose at ${COMPOSE_DIR}; skipping."
fi

echo "Stopping any remaining Docker containers..."
docker stop $(docker ps -q) 2>/dev/null || true

echo "Removing build output from Lesson 10 project..."
rm -rf "${SCRIPT_DIR}/build" \
       "${SCRIPT_DIR}/.gradle" \
       "${SCRIPT_DIR}/.gradle-dist" 2>/dev/null || true

echo "Removing unused Docker resources..."
docker container prune -f 2>/dev/null || true
docker network prune -f 2>/dev/null || true

echo "Cleanup done."
