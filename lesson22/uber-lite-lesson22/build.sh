#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "🔨 Building project: uber-lite-lesson22"
echo ""

if command -v mvn &> /dev/null && command -v java &> /dev/null; then
    echo "Using local Maven installation..."
    mvn clean package -DskipTests
else
    echo "Using Maven Docker image..."
    docker run --rm -v "$SCRIPT_DIR":/workspace -w /workspace maven:3.9-eclipse-temurin-21 mvn clean package -DskipTests
fi

JAR="$SCRIPT_DIR/target/lesson22-metrics-1.0.0.jar"

if [ -f "$JAR" ]; then
    echo ""
    echo "✅ Build successful!"
    echo "  JAR: $JAR"
    ls -lh "$JAR"
else
    echo ""
    echo "❌ Build failed - JAR not found: $JAR"
    exit 1
fi
