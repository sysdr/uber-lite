#!/bin/bash
set -e

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_DIR"

echo "=== Running H3 Spatial Index Demo ==="
echo "Dashboard: http://localhost:8080/dashboard"
echo ""

# Check if metrics server is running
if ! pgrep -f "com.uberlite.MetricsServer" > /dev/null; then
    echo "⚠️  Metrics server not running. Starting it..."
    ./start.sh
    sleep 5
fi

# Check if H3 Spatial Index App is running
if ! pgrep -f "com.uberlite.H3SpatialIndexApp" > /dev/null; then
    echo "⚠️  H3 Spatial Index App not running. Starting it..."
    ./start.sh
    sleep 5
fi

# Stop any existing demo
pkill -f "H3SpatialIndexDemo" 2>/dev/null || true
sleep 1

# Run the demo in continuous mode (background)
echo "Starting continuous data generation..."
CLASSPATH=$(mvn dependency:build-classpath -q -DincludeScope=compile -Dmdep.outputFile=/dev/stdout)
nohup java --enable-preview -cp "target/classes:$CLASSPATH" com.uberlite.H3SpatialIndexDemo --continuous > /tmp/demo-continuous.log 2>&1 &
DEMO_PID=$!
echo "Demo started with PID: $DEMO_PID"
echo ""
echo "✅ Demo running in continuous mode!"
echo ""
echo "📊 Dashboard: http://localhost:8080/dashboard"
echo "📡 Metrics API: http://localhost:8080/api/metrics"
echo ""
echo "Logs: tail -f /tmp/demo-continuous.log"
echo "Stop: pkill -f H3SpatialIndexDemo"

