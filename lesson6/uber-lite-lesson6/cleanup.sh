#!/bin/bash
set -e

echo "🧹 Cleaning up Docker resources and services..."

# Stop Java services
echo "Stopping Java services..."
pkill -f "com.uberlite.MetricsServer" 2>/dev/null || true
pkill -f "com.uberlite.H3SpatialIndexApp" 2>/dev/null || true
pkill -f "H3SpatialIndexDemo" 2>/dev/null || true
sleep 2

# Stop Docker Compose services
echo "Stopping Docker Compose services..."
cd "$(dirname "$0")" 2>/dev/null || cd "$(dirname "${BASH_SOURCE[0]}")"
if [ -f docker-compose.yml ]; then
    docker-compose down 2>/dev/null || true
fi

# Stop and remove Kafka/Zookeeper containers
echo "Stopping and removing Kafka/Zookeeper containers..."
docker ps -a --filter "name=kafka" --format "{{.ID}}" | xargs -r docker stop 2>/dev/null || true
docker ps -a --filter "name=kafka" --format "{{.ID}}" | xargs -r docker rm 2>/dev/null || true
docker ps -a --filter "name=zookeeper" --format "{{.ID}}" | xargs -r docker stop 2>/dev/null || true
docker ps -a --filter "name=zookeeper" --format "{{.ID}}" | xargs -r docker rm 2>/dev/null || true

# Remove unused Docker resources
echo "Removing unused Docker resources..."
docker system prune -f --volumes 2>/dev/null || true

# Clean up log files
echo "Cleaning up log files..."
rm -f /tmp/metrics-server-lesson6.log /tmp/h3-spatial-index-lesson6.log /tmp/demo-continuous.log /tmp/startup-lesson6.log 2>/dev/null || true

# Remove H3 database
echo "Removing H3 spatial index database..."
rm -rf /tmp/h3-spatial-index 2>/dev/null || true

echo ""
echo "✅ Cleanup complete!"
echo ""
echo "Remaining Docker containers:"
docker ps -a 2>/dev/null | head -5 || echo "No containers found"

