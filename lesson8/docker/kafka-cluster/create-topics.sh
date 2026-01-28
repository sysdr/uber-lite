#!/bin/bash
# Create topics with proper replication

set -e

BOOTSTRAP_SERVERS="localhost:19092,localhost:19093,localhost:19094"

echo "Creating topics..."

# Driver location updates (high throughput)
docker exec kafka-1 kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic driver-locations \
  --partitions 12 \
  --replication-factor 3 \
  --config min.insync.replicas=2 \
  --config retention.ms=3600000 \
  --if-not-exists

# Rider location updates
docker exec kafka-1 kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic rider-locations \
  --partitions 12 \
  --replication-factor 3 \
  --config min.insync.replicas=2 \
  --config retention.ms=3600000 \
  --if-not-exists

# Ride matches (lower throughput, higher durability)
docker exec kafka-1 kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic ride-matches \
  --partitions 6 \
  --replication-factor 3 \
  --config min.insync.replicas=2 \
  --config retention.ms=86400000 \
  --if-not-exists

echo "Topics created successfully"

# Describe topics
docker exec kafka-1 kafka-topics --describe \
  --bootstrap-server localhost:9092 \
  --topic driver-locations
