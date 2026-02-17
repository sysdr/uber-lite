#!/usr/bin/env bash
# Verify Firehose metrics endpoint and that dashboard metrics are updating (non-zero after warmup).
set -euo pipefail

METRICS_URL="${METRICS_URL:-http://localhost:8080/metrics}"
MAX_ATTEMPTS="${MAX_ATTEMPTS:-30}"

echo "==> Checking metrics at $METRICS_URL (up to ${MAX_ATTEMPTS}s)"
for i in $(seq 1 "$MAX_ATTEMPTS"); do
  if ! curl -sf --max-time 2 "$METRICS_URL" > /tmp/firehose_metrics.json 2>/dev/null; then
    echo "  Attempt $i/$MAX_ATTEMPTS: endpoint not ready"
    sleep 1
    continue
  fi
  total_produced=$(jq -r '.total_produced // 0' /tmp/firehose_metrics.json 2>/dev/null || echo "0")
  total_consumed=$(jq -r '.total_consumed // 0' /tmp/firehose_metrics.json 2>/dev/null || echo "0")
  produce_throughput=$(jq -r '.produce_throughput_per_sec // 0' /tmp/firehose_metrics.json 2>/dev/null || echo "0")
  consume_throughput=$(jq -r '.consume_throughput_per_sec // 0' /tmp/firehose_metrics.json 2>/dev/null || echo "0")
  total_errors=$(jq -r '.total_errors // 0' /tmp/firehose_metrics.json 2>/dev/null || echo "0")
  p99_latency=$(jq -r '.p99_produce_latency_ms // 0' /tmp/firehose_metrics.json 2>/dev/null || echo "0")

  echo "  total_produced=$total_produced total_consumed=$total_consumed produce_throughput=$produce_throughput consume_throughput=$consume_throughput errors=$total_errors p99_ms=$p99_latency"
  if command -v jq >/dev/null 2>&1; then
    if [[ "$total_produced" =~ ^[0-9]+$ ]] && [[ "$total_consumed" =~ ^[0-9]+$ ]]; then
      if [[ "$total_produced" -gt 0 || "$total_consumed" -gt 0 || "$produce_throughput" -gt 0 || "$consume_throughput" -gt 0 ]]; then
        echo "==> OK: Metrics are updating (non-zero). Dashboard values will reflect these."
        exit 0
      fi
    fi
  else
    # No jq: check for any non-zero metric with grep
    if grep -qE '"(total_produced|total_consumed|produce_throughput_per_sec|consume_throughput_per_sec)": [1-9]' /tmp/firehose_metrics.json 2>/dev/null; then
      echo "==> OK: Metrics are updating (non-zero). Dashboard values will reflect these."
      exit 0
    fi
    if grep -q '"total_produced"' /tmp/firehose_metrics.json; then
      : # valid JSON structure but all zeros yet; keep waiting
    fi
  fi
  sleep 1
done
echo "==> FAIL: Metrics did not show non-zero values within ${MAX_ATTEMPTS}s (or endpoint unreachable)."
exit 1
