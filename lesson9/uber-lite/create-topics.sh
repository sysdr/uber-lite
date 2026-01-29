#!/bin/bash
set -e
echo "Creating driver-locations topic..."
docker exec uber-lite-kafka-1 kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic driver-locations \
  --partitions 16 \
  --replication-factor 3 \
  --config min.insync.replicas=2 \
  --if-not-exists
echo "Topics created successfully"
