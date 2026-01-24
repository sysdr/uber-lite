#!/bin/bash
set -e

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_NAME="uber-lite-lesson5"
PROJECT_PATH="$PROJECT_DIR"

if [ ! -d "$PROJECT_PATH" ]; then
    echo "❌ Project directory not found: $PROJECT_PATH"
    echo "Please run setup.sh first to generate the project"
    exit 1
fi

echo "🔨 Building project: $PROJECT_NAME"
echo "Project path: $PROJECT_PATH"
echo ""

cd "$PROJECT_PATH"

# Check if Maven is available locally, otherwise use Docker
if command -v mvn &> /dev/null && command -v java &> /dev/null; then
    echo "Using local Maven installation..."
    mvn clean package -DskipTests
else
    echo "Using Maven Docker image..."
    docker run --rm -v "$PROJECT_PATH":/workspace -w /workspace maven:3.9 mvn clean package -DskipTests
fi

JAR_FILE="$PROJECT_PATH/target/geohash-limits-1.0-SNAPSHOT.jar"

if [ -f "$JAR_FILE" ]; then
    echo ""
    echo "✅ Build successful!"
    echo "JAR file: $JAR_FILE"
    ls -lh "$JAR_FILE"
else
    echo ""
    echo "❌ Build failed - JAR file not found"
    exit 1
fi

