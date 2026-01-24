#!/bin/bash
set -e

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_DIR"

echo "🚀 Starting Uber-Lite Lesson 5: Geohash Limits"
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
pkill -f "com.uberlite.geohash.demo.MetricsServer" 2>/dev/null || true
pkill -f "com.uberlite.geohash.demo.GeohashLimitsDemo" 2>/dev/null || true
sleep 2

# Start Metrics Server
echo ""
echo "Starting Metrics Server..."
CLASSPATH=$(mvn dependency:build-classpath -q -DincludeScope=compile -Dmdep.outputFile=/dev/stdout)
nohup java -cp "target/classes:$CLASSPATH" com.uberlite.geohash.demo.MetricsServer > /tmp/metrics-server-lesson5.log 2>&1 &
METRICS_PID=$!
echo "Metrics Server started with PID: $METRICS_PID"

# Wait for metrics server to start
sleep 5

echo ""
echo "✅ Services started!"
echo ""
echo "📊 Dashboard: http://localhost:8080/dashboard"
echo "📡 Metrics API: http://localhost:8080/api/metrics"
echo ""
echo "Logs:"
echo "  Metrics Server: tail -f /tmp/metrics-server-lesson5.log"
