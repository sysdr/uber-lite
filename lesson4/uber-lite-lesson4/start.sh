#!/bin/bash
set -e

PROJECT_DIR="/home/systemdrllp5/git/uber-lite/lesson4/uber-lite-lesson4"
cd "$PROJECT_DIR"

echo "🚀 Starting Uber-Lite Lesson 4: Geospatial Indexing with H3"
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

# Ensure topics exist
echo ""
echo "Creating Kafka topics..."
KAFKA_CONTAINER=$(docker ps | grep kafka | awk '{print $1}' | head -1)
if [ -z "$KAFKA_CONTAINER" ]; then
    echo "❌ Kafka container not found"
    exit 1
fi

docker exec "$KAFKA_CONTAINER" kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic driver-locations --partitions 8 --replication-factor 1 2>&1 | grep -v "already exists" || true
docker exec "$KAFKA_CONTAINER" kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic driver-locations-by-h3 --partitions 8 --replication-factor 1 2>&1 | grep -v "already exists" || true

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
pkill -f "com.uberlite.GeoIndexApp" 2>/dev/null || true
pkill -f "com.uberlite.MetricsServer" 2>/dev/null || true
sleep 2

# Start Metrics Server
echo ""
echo "Starting Metrics Server..."
CLASSPATH=$(mvn dependency:build-classpath -q -DincludeScope=compile -Dmdep.outputFile=/dev/stdout)
nohup java -cp "target/classes:$CLASSPATH" com.uberlite.MetricsServer > /tmp/metrics-server-lesson4.log 2>&1 &
METRICS_PID=$!
echo "Metrics Server started with PID: $METRICS_PID"

# Wait for metrics server to start
sleep 5

# Start GeoIndexApp
echo ""
echo "Starting GeoIndexApp..."
nohup java -cp "target/classes:$CLASSPATH" com.uberlite.GeoIndexApp > /tmp/geoindex-app.log 2>&1 &
APP_PID=$!
echo "GeoIndexApp started with PID: $APP_PID"

echo ""
echo "✅ Services started!"
echo ""
echo "📊 Dashboard: http://localhost:8080/dashboard"
echo "📡 Metrics API: http://localhost:8080/api/metrics"
echo ""
echo "Logs:"
echo "  Metrics Server: tail -f /tmp/metrics-server-lesson4.log"
echo "  GeoIndexApp: tail -f /tmp/geoindex-app.log"

