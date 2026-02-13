#!/bin/bash
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

JAR="target/lesson22-metrics-1.0.0.jar"
if [ ! -f "$JAR" ]; then
  echo "❌ $JAR not found. Run ./start.sh first."
  exit 1
fi

echo "Starting MetricsSimulator (produces to Kafka, exposes metrics on :8082)..."
echo "Press Ctrl+C to stop."
exec java -Dcom.sun.management.jmxremote \
  -Dcom.sun.management.jmxremote.port=9999 \
  -Dcom.sun.management.jmxremote.authenticate=false \
  -Dcom.sun.management.jmxremote.ssl=false \
  -jar "$JAR"
