#!/bin/bash

PROJECT_DIR="/home/systemdrllp5/git/uber-lite/lesson4/uber-lite-lesson4"
cd "$PROJECT_DIR"

echo "=== Producing Driver Location Messages ==="
echo "Dashboard: http://localhost:8080/dashboard"
echo ""

# Get Kafka container
KAFKA_CONTAINER=$(docker ps | grep kafka | awk '{print $1}' | head -1)
if [ -z "$KAFKA_CONTAINER" ]; then
    echo "❌ Kafka container not found. Please start Kafka first."
    exit 1
fi

# Produce messages
echo "Producing driver location messages..."
for i in {1..200}; do
    DRIVER_ID=$((i % 100 + 1))
    # Generate realistic NYC coordinates around Manhattan
    LAT=$(echo "40.7$((7000 + i % 1000))" | cut -c1-10)
    LON=$(echo "-73.9$((8000 + i % 1000))" | cut -c1-11)
    TIMESTAMP=$(date +%s)000
    
    MESSAGE="{\"driverId\":$DRIVER_ID,\"lat\":$LAT,\"lon\":$LON,\"timestamp\":$TIMESTAMP,\"status\":\"available\"}"
    
    docker exec "$KAFKA_CONTAINER" sh -c \
      "echo '$MESSAGE' | kafka-console-producer --bootstrap-server localhost:9092 --topic driver-locations" 2>/dev/null
    
    if [ $((i % 20)) -eq 0 ]; then
        echo "  Produced $i messages..."
    fi
    sleep 0.2
done

echo ""
echo "✅ Produced 200 driver location messages"
echo ""
echo "📊 Check dashboard: http://localhost:8080/dashboard"
echo "📡 Metrics API: http://localhost:8080/api/metrics"

