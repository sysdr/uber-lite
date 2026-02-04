#!/bin/bash
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if ! docker ps --format '{{.Names}}' | grep -q 'lesson15-kafka-1'; then
  echo "Kafka not running. Run ./start.sh first."
  exit 1
fi

echo "Building (if needed)..."
./gradlew compileJava -q 2>/dev/null || true
echo "Producing driver location events (batching demo runs ~60 sec)..."
./gradlew runProducer -q --no-daemon
echo "Done. Dashboard will show non-zero counts."
