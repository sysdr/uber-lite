#!/bin/bash
set -e

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_DIR"

echo "=== Running H3 Resolution Strategy Demo ==="
echo "Dashboard: http://localhost:8080/dashboard"
echo ""

# Check if metrics server is running
if ! pgrep -f "com.uberlite.MetricsServer" > /dev/null; then
    echo "⚠️  Metrics server not running. Starting it..."
    "$PROJECT_DIR/start.sh"
    sleep 5
fi

# Check if Resolution Strategy App is running
if ! pgrep -f "com.uberlite.ResolutionStrategyApp" > /dev/null; then
    echo "⚠️  Resolution Strategy App not running. Starting it..."
    "$PROJECT_DIR/start.sh"
    sleep 5
fi

# Stop any existing demo
pkill -f "ResolutionStrategyDemo" 2>/dev/null || true
sleep 1

# Run the demo in continuous mode (background)
echo "Starting continuous data generation..."
CLASSPATH=$(mvn dependency:build-classpath -q -DincludeScope=compile -Dmdep.outputFile=/dev/stdout)
nohup java -cp "target/classes:$CLASSPATH" com.uberlite.ResolutionStrategyDemo --continuous > /tmp/demo-continuous-lesson7.log 2>&1 &
DEMO_PID=$!
echo "Demo started with PID: $DEMO_PID"
echo ""
echo "✅ Demo running in continuous mode!"
echo ""
echo "📊 Dashboard: http://localhost:8080/dashboard"
echo "📡 Metrics API: http://localhost:8080/api/metrics"
echo ""
echo "Logs: tail -f /tmp/demo-continuous-lesson7.log"
echo "Stop: pkill -f ResolutionStrategyDemo"
