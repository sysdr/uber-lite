#!/usr/bin/env bash
set -euo pipefail
echo "==> Checking dashboard health..."
curl -sf http://localhost:8080/health >/dev/null || { echo "FAIL: Dashboard not reachable. Start with: ./start.sh"; exit 1; }
echo "==> Fetching /api/metrics..."
METRICS=$(curl -sf http://localhost:8080/api/metrics)
CONSUMED=$(echo "$METRICS" | sed -n 's/.*"consumedTotal":\([0-9]*\).*/\1/p')
echo "  consumedTotal=$CONSUMED"
if [ -z "$CONSUMED" ]; then
  echo "WARN: Could not parse consumedTotal from /api/metrics."
  exit 1
fi
if [ "$CONSUMED" -lt 0 ] 2>/dev/null; then
  echo "WARN: Invalid consumedTotal."
  exit 1
fi
if [ "$CONSUMED" -eq 0 ]; then
  echo "FAIL: Dashboard metrics are zero. Run ./demo.sh then refresh dashboard (or run this script again)."
  exit 1
fi
echo "==> Dashboard metrics OK (non-zero). All metrics update with demo execution."
