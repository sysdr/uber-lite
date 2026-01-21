#!/bin/bash
set -e

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

cd "$PROJECT_DIR"

# Kill existing MetricsServer
pkill -f "MetricsServer" || true
sleep 2

# Check for Maven
if ! command -v mvn &> /dev/null; then
    echo "❌ Maven not found in PATH. Please install Maven or add it to PATH."
    exit 1
fi

# Build the project
echo "📦 Building project..."
mvn clean package -DskipTests

echo "📊 Starting Metrics Server..."
echo "Dashboard: http://localhost:8080/dashboard"
echo "API: http://localhost:8080/api/metrics"

# Start the server in the background
java -cp "target/lesson-02-log-abstraction-1.0.0.jar:$(mvn dependency:build-classpath -q -Dmdep.outputFile=/dev/stdout)" \
     com.uberlite.MetricsServer > metrics-server.log 2>&1 &

sleep 3
echo "✅ MetricsServer started (PID: $!)"

