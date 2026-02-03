#!/bin/bash
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if ! docker ps --format '{{.Names}}' | grep -q 'lesson13-kafka-1'; then
  echo "❌ Kafka not running. Run ./start.sh first."
  exit 1
fi

echo "Producing driver location events (10k records)..."
mvn exec:java -q
echo "Done. Dashboard will show non-zero counts (refresh in browser or wait ~2s)."
