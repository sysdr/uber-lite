#!/bin/bash
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if ! docker ps --format '{{.Names}}' | grep -q 'lesson14-kafka-1'; then
  echo "❌ Kafka not running. Run ./start.sh first."
  exit 1
fi

echo "Building (if needed)..."
mvn compile -q
echo "Producing driver location events (async demo runs ~30 sec)..."
mvn exec:java -q -Dexec.mainClass="com.uberlite.async.AsyncProducerDemo"
echo "Done. Dashboard will show non-zero counts (refresh in browser or wait ~2s)."
