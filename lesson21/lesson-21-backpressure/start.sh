#!/bin/bash
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "Starting Lesson 21: Backpressure"
echo ""

if ! docker ps --format '{{.Names}}' | grep -q 'lesson-21-backpressure-kafka-1'; then
  echo "Starting Docker cluster..."
  docker compose up -d 2>/dev/null || docker-compose up -d
  echo "Waiting for Kafka (30s)..."
  sleep 30
fi

echo "Building project..."
mvn clean package -DskipTests -q
echo "Build complete."
echo ""
echo "Run ./demo.sh for non-zero dashboard metrics. Dashboard: ./start-dashboard.sh"
