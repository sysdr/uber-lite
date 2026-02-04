#!/bin/bash
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "Starting Lesson 15: Batching Physics"
echo ""

if ! docker ps --format '{{.Names}}' | grep -q 'lesson15-kafka-1'; then
  echo "Starting Docker cluster..."
  docker compose up -d 2>/dev/null || docker-compose up -d
  echo "Waiting for cluster to stabilize (30s)..."
  sleep 30
fi

echo ""
echo "Building project..."
./gradlew compileJava -q 2>/dev/null || ./gradlew build -q --no-daemon
echo "Build complete"
echo ""
echo "Cluster ready. Run ./demo.sh for non-zero dashboard metrics."
echo "Dashboard: ./start-dashboard.sh"
