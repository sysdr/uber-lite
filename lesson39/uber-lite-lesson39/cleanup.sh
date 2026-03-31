#!/usr/bin/env bash
# Stop dashboard, this Compose stack, remove target/logs, prune unused Docker resources.
set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"

echo "==> Stopping MetricsDashboardApp..."
pkill -f "com.uberlite.MetricsDashboardApp" 2>/dev/null || true
sleep 1

echo "==> docker compose down..."
docker compose down --remove-orphans 2>/dev/null || true

echo "==> Removing Maven target/ and logs..."
rm -rf "$DIR/target" 2>/dev/null || true
rm -f "$DIR/dashboard.log" "$DIR/app.log" 2>/dev/null || true

echo "==> Pruning unused Docker objects..."
docker container prune -f 2>/dev/null || true
docker network prune -f 2>/dev/null || true
docker image prune -f 2>/dev/null || true
docker builder prune -f 2>/dev/null || true

echo "==> Done."
