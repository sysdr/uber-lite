#!/bin/bash
set -e

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_DIR"

echo "🚀 Starting Uber-Lite Lesson 6: H3 Spatial Index"
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
pkill -f "com.uberlite.MetricsServer" 2>/dev/null || true
pkill -f "com.uberlite.H3SpatialIndexApp" 2>/dev/null || true
sleep 2

# Start Metrics Server
echo ""
echo "Starting Metrics Server..."
CLASSPATH=$(mvn dependency:build-classpath -q -DincludeScope=compile -Dmdep.outputFile=/dev/stdout)
nohup java --enable-preview -cp "target/classes:$CLASSPATH" com.uberlite.MetricsServer > /tmp/metrics-server-lesson6.log 2>&1 &
METRICS_PID=$!
echo "Metrics Server started with PID: $METRICS_PID"

# Wait for metrics server to start
sleep 5

# Start H3 Spatial Index App
echo ""
echo "Starting H3 Spatial Index App..."
nohup java --enable-preview -cp "target/classes:$CLASSPATH" com.uberlite.H3SpatialIndexApp > /tmp/h3-spatial-index-lesson6.log 2>&1 &
APP_PID=$!
echo "H3 Spatial Index App started with PID: $APP_PID"

# Wait for app to start
sleep 5

echo ""
echo "✅ Services started!"
echo ""
echo "📊 Dashboard: http://localhost:8080/dashboard"
echo "📡 Metrics API: http://localhost:8080/api/metrics"
echo ""
echo "Logs:"
echo "  Metrics Server: tail -f /tmp/metrics-server-lesson6.log"
echo "  H3 Spatial Index App: tail -f /tmp/h3-spatial-index-lesson6.log"

