#!/bin/bash
set -e

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_DIR"

echo "🧪 Running tests for Uber-Lite Lesson 6: H3 Spatial Index"
echo ""

# Test 1: Check if all required files exist
echo "Test 1: Verifying generated files..."
REQUIRED_FILES=(
    "pom.xml"
    "src/main/java/com/uberlite/Models.java"
    "src/main/java/com/uberlite/H3SpatialIndex.java"
    "src/main/java/com/uberlite/DriverLocationConsumer.java"
    "src/main/java/com/uberlite/MatchRequestHandler.java"
    "src/main/java/com/uberlite/H3SpatialIndexApp.java"
    "src/main/java/com/uberlite/MetricsServer.java"
    "src/main/java/com/uberlite/H3SpatialIndexDemo.java"
    "docker-compose.yml"
    "start.sh"
    "test.sh"
    "demo.sh"
)

MISSING_FILES=()
for file in "${REQUIRED_FILES[@]}"; do
    if [ ! -f "$file" ]; then
        MISSING_FILES+=("$file")
    fi
done

if [ ${#MISSING_FILES[@]} -eq 0 ]; then
    echo "✅ All required files exist"
else
    echo "❌ Missing files:"
    for file in "${MISSING_FILES[@]}"; do
        echo "   - $file"
    done
    exit 1
fi

# Test 2: Compile Java code
echo ""
echo "Test 2: Compiling Java code..."
if command -v mvn &> /dev/null; then
    mvn clean compile -q
    if [ $? -eq 0 ]; then
        echo "✅ Compilation successful"
    else
        echo "❌ Compilation failed"
        exit 1
    fi
else
    echo "⚠️  Maven not found in PATH. Skipping compilation tests."
fi

# Test 3: Check if Kafka is accessible
echo ""
echo "Test 3: Checking Kafka connectivity..."
if docker ps | grep -q "kafka"; then
    KAFKA_CONTAINER=$(docker ps | grep kafka | awk '{print $1}' | head -1)
    if docker exec "$KAFKA_CONTAINER" kafka-broker-api-versions --bootstrap-server localhost:9092 &>/dev/null; then
        echo "✅ Kafka is accessible"
    else
        echo "⚠️  Kafka container exists but not responding"
    fi
else
    echo "⚠️  Kafka container not running"
fi

# Test 4: Check if metrics endpoint is accessible (if running)
echo ""
echo "Test 4: Checking metrics server..."
if curl -s http://localhost:8080/api/metrics &>/dev/null; then
    RESPONSE=$(curl -s http://localhost:8080/api/metrics 2>/dev/null)
    if echo "$RESPONSE" | grep -q "driverLocationsIndexed"; then
        echo "✅ Metrics server is running and responding"
    else
        echo "⚠️  Metrics server responding but data not ready"
    fi
else
    echo "⚠️  Metrics server not running (this is OK if not started yet)"
fi

# Test 5: Check for duplicate services
echo ""
echo "Test 5: Checking for duplicate services..."
METRICS_COUNT=$(pgrep -f "com.uberlite.MetricsServer" | wc -l)
APP_COUNT=$(pgrep -f "com.uberlite.H3SpatialIndexApp" | wc -l)

if [ "$METRICS_COUNT" -gt 1 ]; then
    echo "⚠️  Found $METRICS_COUNT MetricsServer processes (should be 0 or 1)"
else
    echo "✅ MetricsServer processes: $METRICS_COUNT"
fi

if [ "$APP_COUNT" -gt 1 ]; then
    echo "⚠️  Found $APP_COUNT H3SpatialIndexApp processes (should be 0 or 1)"
else
    echo "✅ H3SpatialIndexApp processes: $APP_COUNT"
fi

echo ""
echo "✅ All tests completed!"

