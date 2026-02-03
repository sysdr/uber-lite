#!/bin/bash
set -e
cd "$(dirname "$0")"
echo "🧪 Verifying Lesson 14: Asynchronous I/O"
echo ""

if ! docker ps --format '{{.Names}}' | grep -q 'lesson14-kafka-1'; then
  echo "❌ Kafka not running. Run ./start.sh first."
  exit 1
fi

# Check driver-locations topic has records
TOTAL=$(docker exec lesson14-kafka-1 kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list kafka:9092 --topic driver-locations 2>/dev/null | \
  awk -F: '{sum+=$NF} END {print sum+0}' || echo "0")

if [ "$TOTAL" -gt 0 ]; then
  echo "✅ driver-locations topic: $TOTAL records"
else
  echo "⚠️  driver-locations has 0 records. Run: ./demo.sh"
  exit 1
fi

echo ""
echo "✅ Verification passed"
