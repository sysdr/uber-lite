#!/bin/bash
set -e

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

cd "$PROJECT_DIR"

# Check for Maven
if ! command -v mvn &> /dev/null; then
    echo "❌ Maven not found in PATH. Please install Maven or add it to PATH."
    exit 1
fi

# Stop existing MetricsServer if running
pkill -f "MetricsServer" 2>/dev/null || true
sleep 2

# Always rebuild to ensure latest code
echo "📦 Building project..."
mvn clean package -DskipTests

echo "📊 Starting Metrics Server..."
echo "Dashboard: http://localhost:8080/dashboard"
echo "API: http://localhost:8080/api/metrics"

# Start in background
java -cp "target/lesson-02-log-abstraction-1.0.0.jar:$(mvn dependency:build-classpath -q -Dmdep.outputFile=/dev/stdout 2>/dev/null)" \
     com.uberlite.MetricsServer > metrics-server.log 2>&1 &

sleep 3
echo "✅ MetricsServer started (PID: $!)"
echo "📋 Check logs: tail -f metrics-server.log"

