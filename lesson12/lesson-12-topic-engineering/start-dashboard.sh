#!/bin/bash
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
if ! command -v python3 &>/dev/null; then
  echo "python3 not found. Install Python 3 to run the dashboard."
  exit 1
fi
# Stop any existing Lesson 12 dashboard
pkill -f "lesson-12-topic-engineering/dashboard_server.py" 2>/dev/null || true
sleep 2
if command -v ss &>/dev/null; then
  if ss -tlnp 2>/dev/null | grep -q ":8081"; then
    echo "Port 8081 still in use. Run: pkill -f lesson-12-topic-engineering/dashboard_server.py"
    exit 1
  fi
elif command -v netstat &>/dev/null; then
  if netstat -tlnp 2>/dev/null | grep -q ":8081"; then
    echo "Port 8081 still in use. Run: pkill -f lesson-12-topic-engineering/dashboard_server.py"
    exit 1
  fi
fi
echo "Starting Lesson 12 dashboard..."
echo "  Open in browser: http://localhost:8081/dashboard"
echo "  Keep this terminal open. Press Ctrl+C to stop."
echo ""
exec python3 "$SCRIPT_DIR/dashboard_server.py"
