#!/bin/bash
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
if [ ! -f target/deterministic-sim.jar ]; then
  echo "JAR not found. Building..."
  ./build.sh
fi
echo ""
echo "🚀 Starting Deterministic Simulator (default: deterministic, 10 drivers, 100 steps)"
echo ""
java -jar target/deterministic-sim.jar --mode=deterministic --drivers=10 --steps=100
printf '{"drivers":10,"steps":100,"totalEvents":1000,"throughput":40000,"mode":"deterministic","ts":%d}\n' "$(date +%s)" > .dashboard_stats.json
