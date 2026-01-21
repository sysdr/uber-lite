#!/bin/bash
set -e

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_DIR"

echo "🧹 Starting cleanup process..."
echo ""

# Stop all Java services
echo "🛑 Stopping Java services..."
pkill -f "LogConsumer|LogProducer|MetricsServer" 2>/dev/null || echo "  No Java services running"
sleep 2

# Stop Docker containers
echo "🐳 Stopping Docker containers..."
if [ -f "docker-compose.yml" ]; then
    docker-compose down 2>/dev/null || echo "  Docker compose down failed or already stopped"
else
    echo "  docker-compose.yml not found, skipping"
fi

# Remove Docker containers, networks, and volumes
echo "🗑️  Removing Docker resources..."
docker ps -a --filter "name=uber-lite-lesson-02" --format "{{.Names}}" | xargs -r docker rm -f 2>/dev/null || true
docker network prune -f 2>/dev/null || true

# Remove unused Docker images (optional - commented out to avoid removing images in use)
# echo "🗑️  Removing unused Docker images..."
# docker image prune -f 2>/dev/null || true

# Remove unused Docker volumes
echo "🗑️  Removing unused Docker volumes..."
docker volume prune -f 2>/dev/null || true

# Remove build artifacts
echo "📦 Removing build artifacts..."
if [ -d "target" ]; then
    rm -rf target
    echo "  Removed target/ directory"
else
    echo "  target/ directory not found"
fi

# Clean Maven cache (optional - commented out to preserve dependencies)
# echo "🧹 Cleaning Maven cache..."
# mvn clean 2>/dev/null || echo "  Maven clean failed or not needed"

# Remove log files
echo "📋 Cleaning log files..."
find . -maxdepth 1 -name "*.log" -type f -delete 2>/dev/null || true
find . -maxdepth 1 -name "*.run.log" -type f -delete 2>/dev/null || true
find . -maxdepth 1 -name "*.live.log" -type f -delete 2>/dev/null || true
find . -maxdepth 1 -name "*.direct.log" -type f -delete 2>/dev/null || true
find . -maxdepth 1 -name "*-server.log" -type f -delete 2>/dev/null || true
find . -maxdepth 1 -name "*-test.log" -type f -delete 2>/dev/null || true
echo "  Log files cleaned"

# Remove temporary files
echo "🗂️  Cleaning temporary files..."
rm -f /tmp/uberlite.cp 2>/dev/null || true
rm -f /tmp/metrics.json 2>/dev/null || true
echo "  Temporary files cleaned"

echo ""
echo "✅ Cleanup completed successfully!"
echo ""
echo "Summary:"
echo "  - Java services stopped"
echo "  - Docker containers stopped and removed"
echo "  - Docker networks and volumes pruned"
echo "  - Build artifacts removed"
echo "  - Log files cleaned"
echo "  - Temporary files removed"

