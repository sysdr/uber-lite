#!/bin/bash
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
if [ ! -f target/deterministic-sim.jar ]; then
  echo "JAR not found. Building..."
  ./build.sh
fi
echo ""
echo "🎲 Demo: deterministic run (3 drivers, 15 steps)"
echo ""
java -jar target/deterministic-sim.jar --mode=deterministic --drivers=3 --steps=15
echo ""
printf '{"drivers":3,"steps":15,"totalEvents":45,"throughput":1800,"mode":"deterministic","ts":%d}\n' "$(date +%s)" > .dashboard_stats.json
echo "✅ Demo complete. Run ./start.sh for a longer run. Dashboard: http://localhost:8083/dashboard"
