#!/bin/bash
set -e

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "🧪 Running tests for Uber-Lite Lesson 02"
echo ""

cd "$PROJECT_DIR"

# Check for Maven
if ! command -v mvn &> /dev/null; then
    echo "⚠️  Maven not found in PATH. Skipping compilation tests."
    SKIP_COMPILE=true
fi

# Test 1: Check if all required files exist
echo "Test 1: Verifying generated files..."
REQUIRED_FILES=(
    "pom.xml"
    "docker-compose.yml"
    "src/main/java/com/uberlite/DriverLocation.java"
    "src/main/java/com/uberlite/LogProducer.java"
    "src/main/java/com/uberlite/LogConsumer.java"
    "src/main/java/com/uberlite/MetricsServer.java"
)

ALL_FILES_EXIST=true
for file in "${REQUIRED_FILES[@]}"; do
    if [ -f "$file" ]; then
        echo "  ✅ $file"
    else
        echo "  ❌ $file - MISSING"
        ALL_FILES_EXIST=false
    fi
done

if [ "$ALL_FILES_EXIST" = false ]; then
    echo "❌ Some required files are missing!"
    exit 1
fi

# Test 2: Compile Java code
echo ""
echo "Test 2: Compiling Java code..."
if [ "$SKIP_COMPILE" = true ]; then
    echo "  ⚠️  Skipped (Maven not available)"
else
    if mvn clean compile > /dev/null 2>&1; then
        echo "  ✅ Compilation successful"
    else
        echo "  ❌ Compilation failed"
        mvn clean compile
        exit 1
    fi
fi

# Test 3: Check if Kafka is accessible
echo ""
echo "Test 3: Checking Kafka connectivity..."
if timeout 5 bash -c 'cat < /dev/null > /dev/tcp/localhost/9092' 2>/dev/null; then
    echo "  ✅ Kafka is accessible on port 9092"
elif docker ps | grep -q kafka; then
    echo "  ⚠️  Kafka container is running but port may not be ready"
else
    echo "  ⚠️  Kafka is not running (start with: docker-compose up -d)"
fi

# Test 4: Check if metrics endpoint is accessible (if running)
echo ""
echo "Test 4: Checking metrics server..."
if curl -s http://localhost:8080/api/metrics > /dev/null 2>&1; then
    echo "  ✅ Metrics server is accessible"
    METRICS_DATA=$(curl -s http://localhost:8080/api/metrics)
    if echo "$METRICS_DATA" | grep -q "totalCommittedOffset"; then
        echo "  ✅ Metrics API returns valid data"
        echo "  Metrics: $METRICS_DATA"
    else
        echo "  ⚠️  Metrics API returned but format may be incorrect"
    fi
else
    echo "  ⚠️  Metrics server is not running (start with: ./start-metrics.sh)"
fi

# Test 5: Check for duplicate services
echo ""
echo "Test 5: Checking for duplicate services..."
DUPLICATES=$(ps aux | grep -E "LogProducer|LogConsumer|MetricsServer" | grep -v grep | wc -l)
if [ "$DUPLICATES" -gt 0 ]; then
    echo "  ⚠️  Found $DUPLICATES running process(es):"
    ps aux | grep -E "LogProducer|LogConsumer|MetricsServer" | grep -v grep
else
    echo "  ✅ No duplicate services running"
fi

echo ""
echo "✅ All tests completed!"

