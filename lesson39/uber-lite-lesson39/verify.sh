#!/usr/bin/env bash
set -euo pipefail
echo "==> Checking dashboard health..."
curl -sf http://localhost:8080/health >/dev/null || { echo "FAIL: Dashboard not reachable. Start with: ./start.sh"; exit 1; }
echo "==> Waiting for dashboard consumer (retry up to ~2 min)..."
METRICS=""
for attempt in $(seq 1 40); do
  METRICS=$(curl -sf http://localhost:8080/api/metrics) || true
  DRIVER=$(echo "$METRICS" | sed -n 's/.*"driverConsumed":\([0-9]*\).*/\1/p')
  RIDER=$(echo "$METRICS" | sed -n 's/.*"riderConsumed":\([0-9]*\).*/\1/p')
  TOTAL=$(echo "$METRICS" | sed -n 's/.*"consumedTotal":\([0-9]*\).*/\1/p')
  if [ -n "$TOTAL" ] && [ -n "$DRIVER" ] && [ -n "$RIDER" ] \
     && [ "$DRIVER" -gt 0 ] && [ "$RIDER" -gt 0 ] && [ "$TOTAL" -gt 0 ]; then
    echo "  driverConsumed=$DRIVER riderConsumed=$RIDER consumedTotal=$TOTAL (attempt $attempt)"
    echo "==> Dashboard metrics OK (driver, rider, and total non-zero)."
    exit 0
  fi
  sleep 3
done
echo "  last driverConsumed=$DRIVER riderConsumed=$RIDER consumedTotal=$TOTAL"
if [ -z "$TOTAL" ] || [ -z "$DRIVER" ] || [ -z "$RIDER" ]; then
  echo "FAIL: Could not parse metrics JSON."
  exit 1
fi
echo "FAIL: Dashboard metrics are zero. Run ./demo.sh (or POST /run-demo), then ./verify.sh again."
exit 1
