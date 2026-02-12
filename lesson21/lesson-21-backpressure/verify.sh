#!/bin/bash
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "Verifying Lesson 21: Backpressure (driver-locations)"
echo ""

if ! docker ps --format '{{.Names}}' | grep -q 'lesson-21-backpressure-kafka-1'; then
  echo "Kafka not running. Run ./start.sh first."
  exit 1
fi

CONTAINER=$(docker ps --format '{{.Names}}' | grep 'lesson-21-backpressure-kafka-1' | head -1)
SUM=$(docker exec "$CONTAINER" kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list kafka:9092 --topic driver-locations 2>/dev/null | awk -F: '{sum+=$NF} END {print sum+0}' || echo "0")
if [ "${SUM:-0}" -gt 0 ]; then
  echo "driver-locations: $SUM messages"
else
  echo "driver-locations: 0 messages. Run ./demo.sh"
  exit 1
fi

echo ""
echo "Verification passed."
