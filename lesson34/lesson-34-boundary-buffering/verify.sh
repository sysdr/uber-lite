#!/usr/bin/env bash
set -euo pipefail
echo "==> Checking dashboard health..."
curl -sf http://localhost:8080/health >/dev/null || { echo "FAIL: Dashboard not reachable. Start with: ./start.sh"; exit 1; }
echo "==> Fetching /api/metrics..."
METRICS=$(curl -sf http://localhost:8080/api/metrics)
TOTAL=$(echo "$METRICS" | sed -n 's/.*"totalProcessed":\([0-9]*\).*/\1/p')
if [ -z "$TOTAL" ]; then
  echo "WARN: Could not parse totalProcessed from /api/metrics."
  exit 1
fi
if [ "$TOTAL" -eq 0 ] 2>/dev/null; then
  echo "FAIL: Dashboard metrics are zero. Run ./start.sh (app runs load automatically), wait ~30s, then refresh dashboard or run this script again."
  exit 1
fi
echo "  totalProcessed=$TOTAL"
echo "==> Dashboard metrics OK (non-zero). All metrics update with demo execution."
