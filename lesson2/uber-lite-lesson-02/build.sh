#!/bin/bash
set -e

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "🔨 Building Uber-Lite Lesson 02 - Log Abstraction"
echo ""

cd "$PROJECT_DIR"

# Check for Maven
if ! command -v mvn &> /dev/null; then
    echo "❌ Maven not found in PATH. Please install Maven or add it to PATH."
    echo "   Install: sudo apt install maven (Debian/Ubuntu) or brew install maven (macOS)"
    exit 1
fi

# Check Java version
if command -v java &> /dev/null; then
    JAVA_VERSION=$(java -version 2>&1 | head -n 1 | cut -d'"' -f2 | sed '/^1\./s///' | cut -d'.' -f1)
    if [ "$JAVA_VERSION" -lt 21 ]; then
        echo "⚠️  Warning: Java 21 is required, but Java $JAVA_VERSION is installed"
    else
        echo "✅ Java version: $JAVA_VERSION"
    fi
else
    echo "⚠️  Warning: Java not found. Please install Java 21 or later"
fi

echo ""
echo "📦 Building project with Maven..."
echo ""

# Clean and package
mvn clean package

if [ $? -eq 0 ]; then
    echo ""
    echo "✅ Build successful!"
    echo ""
    echo "Generated JAR: target/lesson-02-log-abstraction-1.0.0.jar"
    echo ""
    echo "Next steps:"
    echo "  1. Start services: ./start.sh"
    echo "  2. Or start individually:"
    echo "     - ./start-producer.sh"
    echo "     - ./start-consumer.sh"
    echo "     - ./start-metrics.sh"
else
    echo ""
    echo "❌ Build failed!"
    exit 1
fi

