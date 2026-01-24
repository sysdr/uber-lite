#!/bin/bash
set -e

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_DIR"

echo "=== Running Geohash Limits Demo ==="
echo "Dashboard: http://localhost:8080/dashboard"
echo ""

# Check if metrics server is running
if ! pgrep -f "com.uberlite.geohash.demo.MetricsServer" > /dev/null; then
    echo "⚠️  Metrics server not running. Starting it..."
    ./start.sh
    sleep 5
fi

# Run the demo
echo "Running GeohashLimitsDemo..."
CLASSPATH=$(mvn dependency:build-classpath -q -DincludeScope=compile -Dmdep.outputFile=/dev/stdout)
java -cp "target/classes:$CLASSPATH" com.uberlite.geohash.demo.GeohashLimitsDemo

echo ""
echo "✅ Demo complete!"
echo ""
echo "📊 Check dashboard: http://localhost:8080/dashboard"
echo "📡 Metrics API: http://localhost:8080/api/metrics"
