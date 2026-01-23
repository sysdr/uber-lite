#!/bin/bash

echo "=== Producing Messages & Monitoring Real-Time Metrics ==="
echo "Dashboard: http://localhost:8080/dashboard"
echo ""

# Produce messages in background
(
  for i in {1..100}; do
    docker exec kafka-architecture-lesson-kafka-1-1 sh -c \
      "echo '{\"driverId\":\"D$((i%500+1))\",\"lat\":40.$((7000+i)),\"lon\":-73.$((9000+i)),\"timestamp\":'$(date +%s)'000}' | \
       kafka-console-producer --bootstrap-server localhost:9092 --topic driver-locations" 2>/dev/null
    sleep 0.3
  done
) &

# Monitor metrics
for i in {1..30}; do
  timestamp=$(date +%H:%M:%S)
  result=$(curl -s http://localhost:8080/api/metrics 2>/dev/null | \
    python3 -c "import sys, json; d=json.load(sys.stdin); print(d.get('totalEndOffset', 0))" 2>/dev/null)
  echo "[$timestamp] 📊 Total Messages: ${result:-0}"
  sleep 2
done

echo ""
echo "✅ Check dashboard: http://localhost:8080/dashboard"
echo "✅ Metrics auto-refresh every 2 seconds"

