#!/bin/bash
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "🧪 Verifying Lesson 17: Compression Benchmark"
echo ""

# Check Kafka is running
if ! docker ps --format '{{.Names}}' | grep -q 'compression-benchmark-kafka-1'; then
  echo "❌ Kafka not running. Run ./start.sh first."
  exit 1
fi

# Check topics have records (use project network for internal broker reach)
BROKER_LIST="kafka-1:29092,kafka-2:29093,kafka-3:29094"
for topic in location-events-none location-events-lz4 location-events-snappy location-events-gzip; do
  SUM=$(docker run --rm --network compression-benchmark_default confluentinc/cp-kafka:7.6.0 kafka-run-class kafka.tools.GetOffsetShell \
    --broker-list "$BROKER_LIST" --topic "$topic" 2>/dev/null | awk -F: '{sum+=$NF} END {print sum+0}' || echo "0")
  if [ "${SUM:-0}" -gt 0 ]; then
    echo "✅ $topic: $SUM events"
  else
    echo "⚠️  $topic: 0 events. Run ./demo.sh"
    exit 1
  fi
done

echo ""
echo "✅ Verification passed"
