#!/bin/bash
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
echo "🔨 Building project: lesson-23-deterministic-data"
echo ""
if command -v mvn &>/dev/null && command -v java &>/dev/null; then
    mvn clean package -DskipTests
else
    docker run --rm -v "$SCRIPT_DIR":/workspace -w /workspace maven:3.9-eclipse-temurin-21 mvn clean package -DskipTests
fi
JAR="$SCRIPT_DIR/target/deterministic-sim.jar"
if [ -f "$JAR" ]; then
    echo ""
    echo "✅ Build successful!"
    echo "  JAR: $JAR"
    ls -lh "$JAR"
else
    echo "❌ Build failed - JAR not found: $JAR"
    exit 1
fi
