#!/bin/bash
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "🚀 Starting Uber-Lite Lesson 22 (Metrics Instrumentation)"
echo ""

if ! docker ps --format '{{.Names}}' | grep -q 'lesson22-kafka'; then
  echo "Starting Docker cluster..."
  docker compose up -d 2>/dev/null || docker-compose up -d
  echo "Waiting for Kafka to be ready (15s)..."
  sleep 15
fi

echo "Creating driver-locations topic..."
docker exec lesson22-kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic driver-locations \
  --partitions 1 \
  --replication-factor 1 \
  --if-not-exists 2>/dev/null || true
echo "Topics ready"
echo ""
echo "Building project..."
mvn clean package -DskipTests -q
echo "✅ Build complete"

echo ""
echo "✅ Cluster ready."
echo "   Run ./start-simulator.sh to produce metrics (keep running)."
echo "   Run ./start-dashboard.sh for the dashboard."
