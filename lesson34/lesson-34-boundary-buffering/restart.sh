#!/usr/bin/env bash
# Stop dashboard and main app, then start everything again (Docker + app + dashboard).
set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"
echo "==> Stopping lesson34 apps..."
pkill -f "com.uberlite.boundary.MetricsDashboardApp" 2>/dev/null || true
pkill -f "com.uberlite.boundary.BoundaryBufferingApp" 2>/dev/null || true
sleep 3
echo "==> Restarting (Docker + BoundaryBufferingApp + Dashboard)..."
exec "$DIR/start.sh"
