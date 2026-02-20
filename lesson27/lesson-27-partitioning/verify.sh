#!/usr/bin/env bash
set -euo pipefail
METRICS_URL="${METRICS_URL:-http://localhost:8080/metrics}"
HEALTH_URL="${HEALTH_URL:-http://localhost:8080/health}"
echo "==> Checking health: $HEALTH_URL"
curl -sf "$HEALTH_URL" | grep -q '"status"' && echo "  OK: health endpoint returns status"
echo "==> Checking metrics: $METRICS_URL"
METRICS=$(curl -sf "$METRICS_URL")
echo "$METRICS" | grep -q "streams_state" || { echo "  FAIL: streams_state missing"; exit 1; }
echo "$METRICS" | grep -q "streams_active_tasks" || { echo "  FAIL: streams_active_tasks missing"; exit 1; }
echo "$METRICS" | grep -q "streams_records_consumed_total" || { echo "  FAIL: streams_records_consumed_total missing"; exit 1; }
echo "$METRICS" | grep -q "streams_records_produced_total" || { echo "  FAIL: streams_records_produced_total missing"; exit 1; }
TASKS=$(echo "$METRICS" | sed -n 's/^streams_active_tasks \([0-9]*\).*/\1/p' | head -1)
if [ -n "$TASKS" ] && [ "$TASKS" -gt 0 ]; then
  echo "  OK: streams_active_tasks = $TASKS (non-zero)"
else
  echo "  WARN: streams_active_tasks is 0 or missing (run app and wait for rebalance)"
fi
CONSUMED=$(echo "$METRICS" | sed -n 's/^streams_records_consumed_total \([0-9]*\).*/\1/p' | head -1)
PRODUCED=$(echo "$METRICS" | sed -n 's/^streams_records_produced_total \([0-9]*\).*/\1/p' | head -1)
echo "  OK: streams_records_consumed_total = ${CONSUMED:-0}, streams_records_produced_total = ${PRODUCED:-0}"
if [ "${CONSUMED:-0}" -eq 0 ] && [ "${PRODUCED:-0}" -eq 0 ]; then
  echo "  NOTE: Values are zero. Run ./demo.sh (with app and driver producer running) to see non-zero metrics."
fi
echo "==> Dashboard metrics validation passed."
