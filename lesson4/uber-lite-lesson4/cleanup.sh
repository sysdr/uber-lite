#!/bin/bash
set -e

PROJECT_DIR="/home/systemdrllp5/git/uber-lite/lesson4/uber-lite-lesson4"
cd "$PROJECT_DIR"

echo "🧹 Cleaning up Docker resources and services..."
echo ""

# Stop Java services
echo "1. Stopping Java services..."
pkill -f "com.uberlite.MetricsServer" 2>/dev/null || echo "   No MetricsServer running"
pkill -f "com.uberlite.GeoIndexApp" 2>/dev/null || echo "   No GeoIndexApp running"
sleep 2
echo "✅ Java services stopped"
echo ""

# Stop Docker Compose services
echo "2. Stopping Docker Compose services..."
if [ -f "docker-compose.yml" ]; then
    docker-compose down 2>/dev/null || echo "   Docker Compose services already stopped"
    echo "✅ Docker Compose services stopped"
else
    echo "   docker-compose.yml not found"
fi
echo ""

# Remove unused Docker resources
echo "3. Cleaning up unused Docker resources..."
docker system prune -f --volumes 2>/dev/null || echo "   Docker cleanup completed"
echo "✅ Unused Docker resources removed"
echo ""

# Remove target directory
echo "4. Removing target directory..."
if [ -d "target" ]; then
    rm -rf target
    echo "✅ Target directory removed"
else
    echo "   Target directory does not exist"
fi
echo ""

# Remove log files
echo "5. Cleaning up log files..."
rm -f /tmp/metrics-server-lesson4.log /tmp/geoindex-app.log 2>/dev/null || true
echo "✅ Log files cleaned"
echo ""

echo "✅ Cleanup complete!"
echo ""
echo "Remaining Docker containers:"
docker ps -a 2>/dev/null | grep -E "kafka|zookeeper" || echo "   No Kafka/Zookeeper containers found"

