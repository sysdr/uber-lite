#!/bin/bash
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
echo "Starting Lesson 19 Path Interpolation"
if ! docker ps --format '{{.Names}}' | grep -qE 'lesson19-kafka-1|.*-kafka-1'; then
  echo "Starting Docker cluster..."
  docker compose up -d 2>/dev/null || docker-compose up -d
  echo "Waiting for Kafka (15s)..."
  sleep 15
fi
KAFKA_CONTAINER=$(docker ps --format '{{.Names}}' | grep -E 'kafka|lesson19' | head -1)
[ -n "$KAFKA_CONTAINER" ] || KAFKA_CONTAINER="lesson19-kafka-1"
export KAFKA_CONTAINER
"$SCRIPT_DIR/create-topics.sh"
echo "Building..."
mvn clean package -DskipTests -q
echo "Cluster ready. Run ./demo.sh for data; ./start-dashboard.sh for dashboard."
