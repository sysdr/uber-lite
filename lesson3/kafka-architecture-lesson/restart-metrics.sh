#!/bin/bash
cd "$(dirname "$0")"

echo "Stopping existing MetricsServer..."
pkill -f "com.uberlite.MetricsServer" 2>/dev/null
sleep 2

echo "Compiling..."
mvn clean compile -q

if [ $? -ne 0 ]; then
    echo "Compilation failed!"
    exit 1
fi

echo "Starting MetricsServer..."
CLASSPATH=$(mvn dependency:build-classpath -q -DincludeScope=compile -Dmdep.outputFile=/dev/stdout)
nohup java --enable-preview -cp "target/classes:$CLASSPATH" com.uberlite.MetricsServer > /tmp/metrics-server.log 2>&1 &

sleep 5

echo "Checking status..."
if curl -s http://localhost:8080/api/metrics > /dev/null 2>&1; then
    echo "✅ MetricsServer is running!"
    echo "Dashboard: http://localhost:8080/dashboard"
    echo "Metrics API: http://localhost:8080/api/metrics"
else
    echo "❌ MetricsServer failed to start. Check /tmp/metrics-server.log"
    tail -20 /tmp/metrics-server.log
fi

