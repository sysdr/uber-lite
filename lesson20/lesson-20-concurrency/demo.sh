#!/bin/bash
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if ! docker ps --format '{{.Names}}' | grep -q 'lesson-20-concurrency-kafka-1'; then
  echo "❌ Kafka not running. Run ./start.sh first."
  exit 1
fi

echo "Running Virtual Thread Simulator (produces to driver-locations). Ctrl+C to stop."
echo "Dashboard: http://localhost:8080/dashboard (run ./start-dashboard.sh if not running)"
echo ""
mvn exec:java
