#!/bin/bash
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "Running Lesson 21 tests"
if ! docker ps --format '{{.Names}}' | grep -q 'lesson-21-backpressure-kafka-1'; then
  echo "Kafka not running. Run ./start.sh first."
  exit 1
fi
if [ ! -f target/driver-simulator-1.0.jar ]; then
  echo "JAR not found. Run: mvn clean package"
  exit 1
fi
echo "Running demo (simulator 45s)..."
timeout 45 java -jar target/driver-simulator-1.0.jar 2>/dev/null || [ $? -eq 124 ]
./verify.sh
echo "All tests passed."
