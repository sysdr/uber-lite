#!/bin/bash

echo "=== Kafka Architecture Lesson - Cleanup Script ==="
echo ""

# Stop Java services
echo "1. Stopping Java services..."
pkill -f MetricsServer 2>/dev/null && echo "   ✓ MetricsServer stopped" || echo "   - MetricsServer not running"
pkill -f LocationProducer 2>/dev/null && echo "   ✓ LocationProducer stopped" || echo "   - LocationProducer not running"
pkill -f ClusterInspector 2>/dev/null && echo "   ✓ ClusterInspector stopped" || echo "   - ClusterInspector not running"
echo ""

# Stop Docker containers
echo "2. Stopping Docker containers..."
if [ -f docker-compose.yml ]; then
    docker-compose down 2>/dev/null && echo "   ✓ Docker Compose services stopped" || echo "   - Docker Compose not running"
else
    echo "   - docker-compose.yml not found"
fi

# Stop any remaining Kafka/Zookeeper containers
echo "3. Stopping Kafka and Zookeeper containers..."
CONTAINERS=$(docker ps -a --format "{{.Names}}" | grep -E "kafka|zookeeper" 2>/dev/null)
if [ -n "$CONTAINERS" ]; then
    echo "$CONTAINERS" | xargs docker stop 2>/dev/null && echo "   ✓ Containers stopped" || echo "   - Error stopping containers"
else
    echo "   - No Kafka/Zookeeper containers found"
fi
echo ""

# Remove stopped containers
echo "4. Removing stopped containers..."
CONTAINERS=$(docker ps -a --format "{{.Names}}" | grep -E "kafka|zookeeper" 2>/dev/null)
if [ -n "$CONTAINERS" ]; then
    echo "$CONTAINERS" | xargs docker rm 2>/dev/null && echo "   ✓ Containers removed" || echo "   - Error removing containers"
else
    echo "   - No containers to remove"
fi
echo ""

# Remove unused Docker resources
echo "5. Cleaning up unused Docker resources..."
docker system prune -f --volumes 2>/dev/null && echo "   ✓ Unused Docker resources removed" || echo "   - Error cleaning Docker resources"
echo ""

# Remove target directory
echo "6. Removing Maven target directory..."
if [ -d "target" ]; then
    rm -rf target && echo "   ✓ target directory removed" || echo "   - Error removing target directory"
else
    echo "   - target directory not found"
fi
echo ""

# Remove log files
echo "7. Cleaning up log files..."
rm -f /tmp/metrics-server.log /tmp/producer.log nohup.out 2>/dev/null
rm -f *.log 2>/dev/null
echo "   ✓ Log files cleaned"
echo ""

echo "=== Cleanup Complete ==="
echo ""
echo "Remaining Docker containers:"
docker ps -a --format "  {{.Names}} - {{.Status}}" 2>/dev/null | head -5 || echo "  (none)"
echo ""
echo "To start services again:"
echo "  docker-compose up -d"
echo "  mvn exec:java -Dexec.mainClass=com.uberlite.MetricsServer"

