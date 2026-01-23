#!/bin/bash
set -e

cd /home/systemdrllp5/git/uber-lite/lesson3/kafka-architecture-lesson

echo "=== Fixing MetricsServer Connection Issues ==="

# Step 1: Ensure topic exists
echo "1. Creating topic..."
docker exec kafka-architecture-lesson-kafka-1-1 kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic driver-locations --partitions 12 --replication-factor 3 --config min.insync.replicas=2 2>&1 || true

# Step 2: Stop old server
echo "2. Stopping old MetricsServer..."
pkill -9 -f "com.uberlite.MetricsServer" 2>/dev/null || true
sleep 2

# Step 3: Compile
echo "3. Compiling..."
mvn clean compile -q
if [ ! -f target/classes/com/uberlite/MetricsServer.class ]; then
    echo "❌ Compilation failed - MetricsServer.class not found"
    exit 1
fi
echo "✅ Compilation successful"

# Step 4: Start server
echo "4. Starting MetricsServer..."
CLASSPATH=$(mvn dependency:build-classpath -q -DincludeScope=compile -Dmdep.outputFile=/dev/stdout)
nohup java --enable-preview -cp "target/classes:$CLASSPATH" com.uberlite.MetricsServer > /tmp/metrics-server.log 2>&1 &
METRICS_PID=$!
echo "MetricsServer PID: $METRICS_PID"

# Step 5: Wait and test
echo "5. Waiting for server to start..."
sleep 10

echo "6. Testing connection..."
for i in {1..6}; do
    RESPONSE=$(curl -s http://localhost:8080/api/metrics 2>&1)
    if echo "$RESPONSE" | grep -q "brokerCount"; then
        BROKERS=$(echo "$RESPONSE" | python3 -c "import sys, json; d=json.load(sys.stdin); print(d.get('brokerCount', 0))" 2>/dev/null)
        PARTITIONS=$(echo "$RESPONSE" | python3 -c "import sys, json; d=json.load(sys.stdin); print(d.get('partitionCount', 0))" 2>/dev/null)
        MESSAGES=$(echo "$RESPONSE" | python3 -c "import sys, json; d=json.load(sys.stdin); print(d.get('totalEndOffset', 0))" 2>/dev/null)
        echo "   Attempt $i: Brokers=$BROKERS, Partitions=$PARTITIONS, Messages=$MESSAGES"
        if [ "$BROKERS" -gt 0 ]; then
            echo ""
            echo "✅ SUCCESS! MetricsServer is connected and working!"
            echo "   Dashboard: http://localhost:8080/dashboard"
            echo "   Metrics API: http://localhost:8080/api/metrics"
            exit 0
        fi
    fi
    sleep 3
done

echo ""
echo "⚠️  MetricsServer started but not showing brokers yet."
echo "   Check logs: tail -f /tmp/metrics-server.log"
echo "   Dashboard: http://localhost:8080/dashboard"

