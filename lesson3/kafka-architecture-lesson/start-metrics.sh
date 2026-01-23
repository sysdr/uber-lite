#!/bin/bash
set -e

cd /home/systemdrllp5/git/uber-lite/lesson3/kafka-architecture-lesson

echo "=== Fixing and Starting MetricsServer ==="

# Stop any running instance
echo "1. Stopping old MetricsServer..."
pkill -9 -f "com.uberlite.MetricsServer" 2>/dev/null || true
sleep 2

# Ensure topic exists
echo "2. Ensuring Kafka topic exists..."
docker exec kafka-architecture-lesson-kafka-1-1 kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic driver-locations --partitions 12 --replication-factor 3 --config min.insync.replicas=2 2>&1 | grep -v "already exists" || true

# Compile
echo "3. Compiling..."
mvn clean compile -q
if [ ! -f target/classes/com/uberlite/MetricsServer.class ]; then
    echo "❌ Compilation failed - checking errors..."
    mvn compile 2>&1 | grep -A 3 "ERROR" | head -20
    exit 1
fi
echo "✅ Compilation successful"

# Start server
echo "4. Starting MetricsServer..."
CLASSPATH=$(mvn dependency:build-classpath -q -DincludeScope=compile -Dmdep.outputFile=/dev/stdout)
nohup java --enable-preview -cp "target/classes:$CLASSPATH" com.uberlite.MetricsServer > /tmp/metrics-server.log 2>&1 &
METRICS_PID=$!
echo "MetricsServer started with PID: $METRICS_PID"

# Wait and test
echo "5. Waiting for server to start..."
sleep 12

echo "6. Testing API..."
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
echo "   Check logs: tail -f /tmp/metrics-server.log"
echo "   Dashboard: http://localhost:8080/dashboard"

