#!/usr/bin/env bash
set -e
KAFKA_CONTAINER="lesson24-kafka-1"
BROKER_LIST="kafka-1:29092,kafka-2:29093,kafka-3:29094"
echo "Creating driver-locations topic..."
docker exec "$KAFKA_CONTAINER" kafka-topics --create \
  --bootstrap-server "$BROKER_LIST" \
  --topic driver-locations \
  --partitions 6 \
  --replication-factor 3 \
  --config min.insync.replicas=2 \
  --if-not-exists
echo "Creating driver-locations-processed topic..."
docker exec "$KAFKA_CONTAINER" kafka-topics --create \
  --bootstrap-server "$BROKER_LIST" \
  --topic driver-locations-processed \
  --partitions 6 \
  --replication-factor 3 \
  --config min.insync.replicas=2 \
  --if-not-exists
echo "Topics created successfully."
