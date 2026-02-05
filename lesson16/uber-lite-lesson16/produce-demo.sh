#!/bin/bash
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if ! docker ps --format '{{.Names}}' | grep -q 'uber-lite-lesson16-kafka-1'; then
  echo "❌ Kafka not running. Run ./start.sh first."
  exit 1
fi

echo "Producing driver location events via LingerBenchmark..."
if [ -f target/lesson16-linger-ms-1.0.0.jar ]; then
  java --enable-preview -jar target/lesson16-linger-ms-1.0.0.jar baseline
  java --enable-preview -jar target/lesson16-linger-ms-1.0.0.jar optimized
else
  echo "❌ JAR not found. Run: mvn clean package"
  exit 1
fi
echo "Done. Dashboard will show non-zero counts (refresh in browser or wait ~5s)."
