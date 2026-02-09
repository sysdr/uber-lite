#!/bin/bash
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if ! docker ps --format '{{.Names}}' | grep -q 'kafka-1'; then
  echo "❌ Kafka not running. Run ./start.sh first."
  exit 1
fi

if [ ! -f target/driver-model-1.0.0.jar ]; then
  echo "❌ JAR not found. Run: mvn clean package"
  exit 1
fi

echo "Running Driver Simulator (produces to driver-locations for 45 seconds)..."
java -jar target/driver-model-1.0.0.jar &
PID=$!
sleep 45
kill $PID 2>/dev/null || true
wait $PID 2>/dev/null || true
echo "Done. Dashboard will show non-zero counts (refresh in browser or wait ~5s)."
