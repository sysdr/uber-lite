#!/bin/bash
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
echo "Verifying Lesson 15: Batching Physics"
echo ""

if ! docker ps --format '{{.Names}}' | grep -q 'lesson15-kafka-1'; then
  echo "Kafka not running. Run ./start.sh first."
  exit 1
fi

TOTAL=$(docker exec lesson15-kafka-1 kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list kafka:9092 --topic driver-locations 2>/dev/null | \
  awk -F: '{sum+=$NF} END {print sum+0}' || echo "0")

if [ "$TOTAL" -gt 0 ]; then
  echo "driver-locations topic: $TOTAL records"
else
  echo "driver-locations has 0 records. Run: ./demo.sh"
  exit 1
fi

echo ""
echo "Running unit/integration tests..."
./gradlew test -q --no-daemon
echo ""
echo "Verification passed"
