#!/bin/bash
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "🚀 Starting Lesson 18: H3-Aware Driver Model"
echo ""

if ! docker ps --format '{{.Names}}' | grep -q 'kafka-1'; then
  echo "Starting Docker cluster..."
  docker compose up -d 2>/dev/null || docker-compose up -d
  echo "Waiting for Kafka to be ready (20s)..."
  sleep 20
fi

echo ""
echo "Building project..."
mvn clean package -DskipTests -q
echo "✅ Build complete"

echo ""
echo "✅ Cluster ready. Run ./demo.sh for non-zero dashboard metrics."
echo "   Dashboard: ./start-dashboard.sh"
