#!/bin/bash
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
KAFKA_CONTAINER="uber-lite-lesson16-kafka-1"
echo "Creating driver-locations topic..."
docker exec "$KAFKA_CONTAINER" kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic driver-locations \
  --partitions 4 \
  --replication-factor 1 \
  --if-not-exists
echo "Topics created successfully"
