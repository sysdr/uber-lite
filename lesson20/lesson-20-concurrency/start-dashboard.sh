#!/bin/bash
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
if ! command -v python3 &>/dev/null; then
  echo "python3 not found. Install Python 3 to run the dashboard."
  exit 1
fi
pkill -f "dashboard_server.py" 2>/dev/null || true
sleep 1
if command -v ss &>/dev/null; then
  if ss -tlnp 2>/dev/null | grep -q ":8080 "; then
    echo "Port 8080 still in use. Run: pkill -f dashboard_server.py"
    exit 1
  fi
fi
echo "Starting Lesson 20 dashboard..."
echo "  Open in browser: http://localhost:8080/dashboard"
echo "  Keep this terminal open. Press Ctrl+C to stop."
echo ""
exec python3 "$SCRIPT_DIR/dashboard_server.py"
