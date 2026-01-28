#!/bin/bash
# Start Lesson 8 dashboard at http://localhost:8080/dashboard
# Requires: cluster running (./start.sh). Run produce-demo.sh for non-zero topic counts.

set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if ! command -v python3 &>/dev/null; then
  echo "python3 not found. Install Python 3 to run the dashboard."
  exit 1
fi

PORT=8080
if command -v ss &>/dev/null; then
  if ss -tlnp 2>/dev/null | grep -q ":$PORT "; then
    echo "Port $PORT is already in use. Stop the other process (e.g. another dashboard or MetricsServer) and try again."
    exit 1
  fi
elif command -v nc &>/dev/null; then
  if nc -z localhost $PORT 2>/dev/null; then
    echo "Port $PORT is already in use. Stop the other process and try again."
    exit 1
  fi
fi

echo "Starting Lesson 8 dashboard on http://localhost:$PORT"
echo "  Dashboard: http://localhost:$PORT/dashboard"
echo "  API:       http://localhost:$PORT/api/metrics"
echo "Press Ctrl+C to stop."
echo ""

exec python3 "$SCRIPT_DIR/dashboard_server.py"
