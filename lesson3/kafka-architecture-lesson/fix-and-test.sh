#!/bin/bash
cd "$(dirname "$0")"

echo "=== Step 1: Checking Kafka ==="
docker-compose ps | grep kafka | head -3

echo ""
echo "=== Step 2: Creating topic if needed ==="
docker exec kafka-architecture-lesson-kafka-1-1 kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic driver-locations --partitions 12 --replication-factor 3 --config min.insync.replicas=2 2>&1

echo ""
echo "=== Step 3: Verifying topic ==="
docker exec kafka-architecture-lesson-kafka-1-1 kafka-topics --bootstrap-server localhost:9092 --list 2>&1 | grep driver-locations

echo ""
echo "=== Step 4: Stopping MetricsServer ==="
pkill -9 -f "com.uberlite.MetricsServer" 2>/dev/null
sleep 2

echo ""
echo "=== Step 5: Compiling ==="
mvn clean compile -q
if [ $? -ne 0 ]; then
    echo "❌ Compilation failed!"
    exit 1
fi
echo "✅ Compilation successful"

echo ""
echo "=== Step 6: Starting MetricsServer ==="
CLASSPATH=$(mvn dependency:build-classpath -q -DincludeScope=compile -Dmdep.outputFile=/dev/stdout)
nohup java --enable-preview -cp "target/classes:$CLASSPATH" com.uberlite.MetricsServer > /tmp/metrics-server.log 2>&1 &
METRICS_PID=$!
echo "MetricsServer PID: $METRICS_PID"

echo ""
echo "=== Step 7: Waiting for server to start ==="
sleep 8

echo ""
echo "=== Step 8: Testing API ==="
for i in {1..5}; do
    echo "Attempt $i:"
    RESPONSE=$(curl -s http://localhost:8080/api/metrics 2>&1)
    if [ $? -eq 0 ] && [ ! -z "$RESPONSE" ]; then
        echo "$RESPONSE" | python3 -c "import sys, json; d=json.load(sys.stdin); print(f\"  Brokers: {d.get('brokerCount', 0)}, Partitions: {d.get('partitionCount', 0)}, Messages: {d.get('totalEndOffset', 0)}\")" 2>&1
        if [ ${d.get('brokerCount', 0)} -gt 0 ]; then
            echo "✅ MetricsServer is working!"
            break
        fi
    else
        echo "  Waiting for server..."
    fi
    sleep 2
done

echo ""
echo "=== Step 9: Checking logs ==="
tail -20 /tmp/metrics-server.log

echo ""
echo "=== Dashboard URL ==="
echo "http://localhost:8080/dashboard"

