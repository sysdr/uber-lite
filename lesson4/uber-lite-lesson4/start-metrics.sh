#!/bin/bash
set -e

PROJECT_DIR="/home/systemdrllp5/git/uber-lite/lesson4/uber-lite-lesson4"
cd "$PROJECT_DIR"

echo "=== Starting Metrics Server ==="

# Stop any existing instance
echo "1. Stopping old MetricsServer..."
pkill -9 -f "com.uberlite.MetricsServer" 2>/dev/null || true
sleep 2

# Ensure Kafka is running
if ! docker ps | grep -q "kafka"; then
    echo "2. Starting Kafka..."
    docker-compose up -d
    sleep 10
else
    echo "2. Kafka is already running"
fi

# Ensure topics exist
echo "3. Ensuring Kafka topics exist..."
KAFKA_CONTAINER=$(docker ps | grep kafka | awk '{print $1}' | head -1)
if [ -n "$KAFKA_CONTAINER" ]; then
    docker exec "$KAFKA_CONTAINER" kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic driver-locations --partitions 8 --replication-factor 1 2>&1 | grep -v "already exists" || true
    docker exec "$KAFKA_CONTAINER" kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic driver-locations-by-h3 --partitions 8 --replication-factor 1 2>&1 | grep -v "already exists" || true
fi

# Compile
echo "4. Compiling..."
mvn clean compile -q
if [ ! -f target/classes/com/uberlite/MetricsServer.class ]; then
    echo "❌ Compilation failed - checking errors..."
    mvn compile 2>&1 | grep -A 3 "ERROR" | head -20
    exit 1
fi
echo "✅ Compilation successful"

# Start server
echo "5. Starting MetricsServer..."
CLASSPATH=$(mvn dependency:build-classpath -q -DincludeScope=compile -Dmdep.outputFile=/dev/stdout)
nohup java -cp "target/classes:$CLASSPATH" com.uberlite.MetricsServer > /tmp/metrics-server-lesson4.log 2>&1 &
METRICS_PID=$!
echo "MetricsServer started with PID: $METRICS_PID"

# Wait and test
echo "6. Waiting for server to start..."
sleep 12

echo "7. Testing API..."
for i in {1..5}; do
    RESPONSE=$(curl -s http://localhost:8080/api/metrics 2>&1)
    if echo "$RESPONSE" | grep -q "brokerCount"; then
        BROKERS=$(echo "$RESPONSE" | python3 -c "import sys, json; d=json.load(sys.stdin); print(d.get('brokerCount', 0))" 2>/dev/null || echo "0")
        PARTITIONS=$(echo "$RESPONSE" | python3 -c "import sys, json; d=json.load(sys.stdin); print(d.get('partitionCount', 0))" 2>/dev/null || echo "0")
        MESSAGES=$(echo "$RESPONSE" | python3 -c "import sys, json; d=json.load(sys.stdin); print(d.get('totalEndOffset', 0))" 2>/dev/null || echo "0")
        echo "   Attempt $i: Brokers=$BROKERS, Partitions=$PARTITIONS, Messages=$MESSAGES"
        if [ "$BROKERS" -gt 0 ]; then
            echo ""
            echo "✅ SUCCESS! MetricsServer is working!"
            echo ""
            echo "📊 Dashboard: http://localhost:8080/dashboard"
            echo "📡 Metrics API: http://localhost:8080/api/metrics"
            exit 0
        fi
    else
        echo "   Attempt $i: Server not ready yet..."
    fi
    sleep 3
done

echo ""
echo "⚠️  Server started but metrics not updating yet."
echo "   Check logs: tail -f /tmp/metrics-server-lesson4.log"
echo "   Dashboard: http://localhost:8080/dashboard"

