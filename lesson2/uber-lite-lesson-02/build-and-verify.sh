#!/bin/bash
set -e

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "🔍 Verifying Prerequisites..."
echo ""

cd "$PROJECT_DIR"

# Check Java
if ! command -v java &> /dev/null; then
    echo "❌ Java not found. Please install: sudo apt install -y openjdk-21-jdk"
    exit 1
fi
JAVA_VERSION=$(java -version 2>&1 | head -n 1)
echo "✅ $JAVA_VERSION"

# Check Maven
if ! command -v mvn &> /dev/null; then
    echo "❌ Maven not found. Please install: sudo apt install -y maven"
    exit 1
fi
MVN_VERSION=$(mvn -version | head -n 1)
echo "✅ $MVN_VERSION"

echo ""
echo "🔨 Building project..."
echo ""

# Build
mvn clean package

if [ $? -eq 0 ]; then
    echo ""
    echo "✅ Build successful!"
    echo ""
    if [ -f "target/lesson-02-log-abstraction-1.0.0.jar" ]; then
        JAR_SIZE=$(ls -lh target/lesson-02-log-abstraction-1.0.0.jar | awk '{print $5}')
        echo "Generated JAR: target/lesson-02-log-abstraction-1.0.0.jar ($JAR_SIZE)"
    fi
    echo ""
    echo "✅ All verification complete!"
else
    echo ""
    echo "❌ Build failed!"
    exit 1
fi

