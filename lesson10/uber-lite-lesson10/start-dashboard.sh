#!/bin/bash
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if ! command -v python3 &>/dev/null; then
  echo "python3 not found. Install Python 3 to run the dashboard."
  exit 1
fi

# Stop any existing Lesson 10 dashboard
pkill -f "dashboard_server.py" 2>/dev/null || true
pkill -f "uber-lite-lesson10/dashboard_server.py" 2>/dev/null || true
sleep 1

# Free port 8081 if still in use
if command -v ss &>/dev/null; then
  if ss -tlnp 2>/dev/null | grep -q ":8081 "; then
    echo "Port 8081 in use. Stopping process..."
    pkill -f "dashboard_server.py" 2>/dev/null || true
    sleep 2
  fi
fi

echo "Starting Lesson 10 dashboard..."
echo "  Dashboard: http://localhost:8081/dashboard"
echo "  Press Ctrl+C to stop."
echo ""
exec python3 "$SCRIPT_DIR/dashboard_server.py"
