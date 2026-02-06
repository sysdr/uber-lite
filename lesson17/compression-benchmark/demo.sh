#!/bin/bash
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if ! docker ps --format '{{.Names}}' | grep -q 'compression-benchmark-kafka-1'; then
  echo "❌ Kafka not running. Run ./start.sh first."
  exit 1
fi

echo "Running Compression Benchmark (produces to location-events-*)..."
if [ -f target/compression-benchmark-1.0.jar ]; then
  java -jar target/compression-benchmark-1.0.jar
else
  echo "❌ JAR not found. Run: mvn clean package"
  exit 1
fi
echo "Done. Dashboard will show non-zero counts (refresh in browser or wait ~5s)."
