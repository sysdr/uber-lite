#!/usr/bin/env bash
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if ! docker ps --format '{{.Names}}' | grep -q 'lesson24-kafka-1'; then
  echo "Kafka not running. Run ./start.sh first."
  exit 1
fi

JAR="$SCRIPT_DIR/target/stress-test-1.0.0.jar"
if [ ! -f "$JAR" ]; then
  echo "JAR not found. Run ./start.sh first to build."
  exit 1
fi

echo "Running stress test demo (~90s) to update dashboard metrics..."
# Run stress test for 90s then stop so dashboard shows non-zero metrics
export TEST_DURATION_MINUTES=1
timeout 95 java -XX:+UseContainerSupport -XX:MaxRAMPercentage=70.0 -jar "$JAR" 2>/dev/null || true
echo "Demo finished. Dashboard at http://localhost:8080/dashboard should show updated metrics."
