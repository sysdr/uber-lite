#!/bin/bash
set -e
KAFKA_CONTAINER="${KAFKA_CONTAINER:-lesson19-kafka-1}"
echo "Creating driver-locations topic..."
docker exec "$KAFKA_CONTAINER" kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic driver-locations \
  --partitions 4 \
  --replication-factor 1 \
  --if-not-exists
echo "Topics created successfully"
