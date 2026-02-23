#!/usr/bin/env bash
set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"

echo "==> Stopping dashboard app (if running)..."
pkill -f "com.uberlite.MetricsDashboardApp" 2>/dev/null || true

echo "==> Stopping Docker Compose containers..."
docker compose down --remove-orphans 2>/dev/null || true

echo "==> Removing unused Docker resources..."
docker system prune -f 2>/dev/null || true

echo "==> Cleanup done."
