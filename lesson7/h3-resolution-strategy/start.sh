#!/bin/bash
set -e

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_DIR"

echo "🚀 Starting Uber-Lite Lesson 7: H3 Resolution Strategy"
echo ""

# Check if docker-compose is running
if ! docker ps | grep -q "kafka"; then
    echo "Starting Docker Compose services..."
    docker-compose up -d
    echo "Waiting for Kafka to be ready..."
    sleep 10
else
    echo "✅ Kafka is already running"
fi

# Build the project
echo ""
echo "Building project..."
if command -v mvn &> /dev/null; then
    mvn clean package -DskipTests
    if [ $? -ne 0 ]; then
        echo "❌ Build failed"
        exit 1
    fi
else
    echo "❌ Maven not found"
    exit 1
fi

# Stop any existing instances
echo ""
echo "Stopping any existing instances..."
pkill -9 -f "com.uberlite.MetricsServer" 2>/dev/null || true
pkill -9 -f "com.uberlite.ResolutionStrategyApp" 2>/dev/null || true
pkill -9 -f "com.uberlite.ResolutionStrategyDemo" 2>/dev/null || true
sleep 3

# Start Metrics Server
echo ""
echo "Starting Metrics Server..."
CLASSPATH=$(mvn dependency:build-classpath -q -DincludeScope=compile -Dmdep.outputFile=/dev/stdout)
nohup java -cp "target/classes:$CLASSPATH" com.uberlite.MetricsServer > /tmp/metrics-server-lesson7.log 2>&1 &
METRICS_PID=$!
echo "Metrics Server started with PID: $METRICS_PID"

# Wait for metrics server to start
sleep 5

# Start Resolution Strategy App
echo ""
echo "Starting Resolution Strategy App..."
nohup java -cp "target/classes:$CLASSPATH" com.uberlite.ResolutionStrategyApp > /tmp/resolution-strategy-lesson7.log 2>&1 &
APP_PID=$!
echo "Resolution Strategy App started with PID: $APP_PID"

# Start continuous data producer (driver locations + match results + benchmark results)
echo ""
echo "Starting continuous metrics producer (ResolutionStrategyDemo)..."
nohup java -cp "target/classes:$CLASSPATH" com.uberlite.ResolutionStrategyDemo --continuous-only > /tmp/demo-lesson7.log 2>&1 &
DEMO_PID=$!
echo "Continuous producer started with PID: $DEMO_PID"

# Wait for services to start
sleep 5

echo ""
echo "✅ Services started!"
echo ""
echo "📊 Dashboard: http://localhost:8080/dashboard"
echo "📡 Metrics API: http://localhost:8080/api/metrics"
echo ""
echo "Logs:"
echo "  Metrics Server: tail -f /tmp/metrics-server-lesson7.log"
echo "  Resolution Strategy App: tail -f /tmp/resolution-strategy-lesson7.log"
echo "  Continuous producer: tail -f /tmp/demo-lesson7.log"
