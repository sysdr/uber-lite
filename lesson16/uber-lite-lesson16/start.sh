#!/bin/bash
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "🚀 Starting Uber-Lite Lesson 16 (linger.ms Benchmark)"
echo ""

if ! docker ps --format '{{.Names}}' | grep -q 'uber-lite-lesson16-kafka-1'; then
  echo "Starting Docker cluster..."
  docker compose up -d 2>/dev/null || docker-compose up -d
  echo "Waiting for Kafka to be ready (15s)..."
  sleep 15
fi

./create-topics.sh

echo ""
echo "Building project..."
mvn clean package -DskipTests -q
echo "✅ Build complete"

echo ""
echo "✅ Cluster ready. Run ./produce-demo.sh for non-zero dashboard metrics."
echo "   Dashboard: ./start-dashboard.sh"
