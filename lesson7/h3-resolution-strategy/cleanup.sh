#!/bin/bash
set -e

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_DIR"

echo "🧹 Lesson 7 Cleanup: Stopping services and removing Docker resources"
echo ""

# Stop Lesson 7 Java processes
echo "Stopping Java services..."
pkill -9 -f "com.uberlite.MetricsServer" 2>/dev/null || true
pkill -9 -f "com.uberlite.ResolutionStrategyApp" 2>/dev/null || true
pkill -9 -f "com.uberlite.ResolutionStrategyDemo" 2>/dev/null || true
sleep 2
echo "✓ Java processes stopped"

# Stop Docker Compose stack (zookeeper, kafka)
echo ""
echo "Stopping Docker Compose services..."
if docker compose version &>/dev/null; then
    docker compose down 2>/dev/null || true
elif command -v docker-compose &>/dev/null; then
    docker-compose down 2>/dev/null || true
fi
echo "✓ Containers stopped"

# Remove unused Docker resources (images, networks, volumes not used by any container)
echo ""
echo "Removing unused Docker resources..."
docker system prune -f
echo "✓ Unused Docker resources removed"

echo ""
echo "✅ Cleanup complete"
