#!/bin/bash
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "🚀 Starting Lesson 20 Concurrency"
echo ""

KAFKA_CONTAINER="lesson-20-concurrency-kafka-1"
if ! docker ps --format '{{.Names}}' | grep -q "$KAFKA_CONTAINER"; then
  echo "Starting Docker (Kafka + Zookeeper)..."
  (docker compose up -d 2>/dev/null || docker-compose up -d)
  echo "Waiting for Kafka to be ready (15s)..."
  sleep 15
fi

echo "Creating driver-locations topic (if needed)..."
docker exec "$KAFKA_CONTAINER" kafka-topics --create \
  --bootstrap-server kafka:9092 \
  --topic driver-locations \
  --partitions 4 \
  --replication-factor 1 \
  --if-not-exists 2>/dev/null || true

echo ""
echo "Building project..."
mvn clean package -DskipTests -q
echo "✅ Build complete"

echo ""
echo "✅ Cluster ready. Run ./demo.sh for non-zero dashboard metrics."
echo "   Dashboard: ./start-dashboard.sh"
