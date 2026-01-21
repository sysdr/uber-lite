#!/bin/bash
set -e

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MODE="${1:-all}"

echo "🚀 Starting Uber-Lite Lesson 02 - Log Abstraction"
echo "Mode: $MODE"
echo ""

cd "$PROJECT_DIR"

# Check for Maven
if ! command -v mvn &> /dev/null; then
    echo "❌ Maven not found in PATH. Please install Maven or add it to PATH."
    echo "   Install: sudo apt install maven (Debian/Ubuntu) or brew install maven (macOS)"
    exit 1
fi

# Check if docker-compose is running
if ! docker ps | grep -q "kafka\|zookeeper"; then
    echo "⚠️  Docker services not running. Starting docker-compose..."
    docker-compose up -d
    echo "⏳ Waiting for Kafka to be ready..."
    sleep 10
fi

# Check for duplicate services
echo "🔍 Checking for running Java processes..."
DUPLICATES=$(ps aux | grep -E "LogProducer|LogConsumer|MetricsServer" | grep -v grep | wc -l)
if [ "$DUPLICATES" -gt 0 ]; then
    echo "⚠️  Found $DUPLICATES existing process(es). Killing duplicates..."
    pkill -f "LogProducer|LogConsumer|MetricsServer" || true
    sleep 2
fi

# Build the project
if [ ! -d "target" ] || [ ! -f "target/lesson-02-log-abstraction-1.0.0.jar" ]; then
    echo "📦 Building project..."
    mvn clean package -DskipTests
fi

case "$MODE" in
    producer)
        echo "📤 Starting Producer..."
        mvn exec:java -Dexec.mainClass="com.uberlite.LogProducer"
        ;;
    consumer)
        echo "📥 Starting Consumer..."
        mvn exec:java -Dexec.mainClass="com.uberlite.LogConsumer"
        ;;
    metrics)
        echo "📊 Starting Metrics Server..."
        mvn exec:java -Dexec.mainClass="com.uberlite.MetricsServer"
        ;;
    all|*)
        echo "🚀 Starting all services..."
        
        # Start metrics server in background
        echo "📊 Starting Metrics Server in background..."
        mvn exec:java -Dexec.mainClass="com.uberlite.MetricsServer" > metrics.log 2>&1 &
        METRICS_PID=$!
        echo "Metrics Server PID: $METRICS_PID"
        sleep 3
        
        # Start consumer in background
        echo "📥 Starting Consumer in background..."
        mvn exec:java -Dexec.mainClass="com.uberlite.LogConsumer" > consumer.log 2>&1 &
        CONSUMER_PID=$!
        echo "Consumer PID: $CONSUMER_PID"
        sleep 3
        
        # Start producer (foreground)
        echo "📤 Starting Producer..."
        mvn exec:java -Dexec.mainClass="com.uberlite.LogProducer"
        
        # Wait a bit for processing
        echo "⏳ Waiting for messages to be processed..."
        sleep 5
        
        echo ""
        echo "✅ Services started:"
        echo "  - Metrics Server: http://localhost:8080/dashboard"
        echo "  - Metrics API: http://localhost:8080/api/metrics"
        echo "  - Consumer PID: $CONSUMER_PID"
        echo "  - Metrics PID: $METRICS_PID"
        echo ""
        echo "Press Ctrl+C to stop all services"
        
        # Cleanup on exit
        trap "echo 'Stopping services...'; kill $CONSUMER_PID $METRICS_PID 2>/dev/null || true; exit" INT TERM
        wait
        ;;
esac

