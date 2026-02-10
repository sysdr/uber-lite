#!/bin/bash
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
if ! docker ps --format '{{.Names}}' | grep -qE 'kafka|lesson19'; then
  echo "Kafka not running. Run: $SCRIPT_DIR/start.sh"
  exit 1
fi
echo "Producing driver location events..."
mvn -q exec:java -Dexec.mainClass="com.uberlite.pathinterpolation.DataGenerator" 2>/dev/null || true
echo "Done. Refresh dashboard for non-zero metrics."
