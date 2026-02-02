#!/bin/bash
# topics_init.sh - Idempotent topic provisioning for Uber-Lite

set -e

KAFKA_CONTAINER="${KAFKA_CONTAINER:-uber-lite-kafka-1}"
BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka-1:29092,kafka-2:29092,kafka-3:29092}"
REPLICATION_FACTOR="${REPLICATION_FACTOR:-3}"

echo "📋 Initializing Uber-Lite topics (container=$KAFKA_CONTAINER)"

# Wait for Kafka to be ready
echo "⏳ Waiting for Kafka cluster..."
timeout 60 bash -c "until docker exec $KAFKA_CONTAINER kafka-broker-api-versions --bootstrap-server $BOOTSTRAP &>/dev/null; do sleep 2; done" || {
    echo "❌ Kafka cluster not ready after 60s"
    exit 1
}

# Create driver-locations topic (high throughput)
echo "📍 Creating driver-locations topic (60 partitions)..."
docker exec $KAFKA_CONTAINER kafka-topics --create --if-not-exists \
  --bootstrap-server $BOOTSTRAP \
  --topic driver-locations \
  --partitions 60 \
  --replication-factor $REPLICATION_FACTOR \
  --config retention.ms=86400000 \
  --config segment.ms=3600000 \
  --config compression.type=lz4 \
  --config min.insync.replicas=2

# Create rider-requests topic (low throughput)
echo "🚗 Creating rider-requests topic (12 partitions)..."
docker exec $KAFKA_CONTAINER kafka-topics --create --if-not-exists \
  --bootstrap-server $BOOTSTRAP \
  --topic rider-requests \
  --partitions 12 \
  --replication-factor $REPLICATION_FACTOR \
  --config retention.ms=3600000 \
  --config min.insync.replicas=2

# Create ride-matches topic (output stream)
echo "🎯 Creating ride-matches topic (12 partitions)..."
docker exec $KAFKA_CONTAINER kafka-topics --create --if-not-exists \
  --bootstrap-server $BOOTSTRAP \
  --topic ride-matches \
  --partitions 12 \
  --replication-factor $REPLICATION_FACTOR \
  --config retention.ms=3600000

echo "✅ Topic initialization complete"
echo ""
echo "📊 Topic Summary:"
docker exec $KAFKA_CONTAINER kafka-topics --list --bootstrap-server $BOOTSTRAP | grep -E "(driver-locations|rider-requests|ride-matches)"
