#!/bin/bash
set -e

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

cd "$PROJECT_DIR"

# Check for Maven
if ! command -v mvn &> /dev/null; then
    echo "❌ Maven not found in PATH. Please install Maven or add it to PATH."
    exit 1
fi

# Check if project is built
if [ ! -d "target" ] || [ ! -f "target/lesson-02-log-abstraction-1.0.0.jar" ]; then
    echo "📦 Building project..."
    mvn clean package -DskipTests
fi

echo "📤 Starting Producer..."
mvn exec:java -Dexec.mainClass="com.uberlite.LogProducer"

