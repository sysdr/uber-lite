#!/bin/bash
set -e

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JAR_FILE="$PROJECT_DIR/target/lesson-01-monolith-vs-eventlog-1.0.0.jar"
MODE="${1:-both}"

if [ ! -f "$JAR_FILE" ]; then
    echo "❌ JAR file not found: $JAR_FILE"
    echo "Please build the project first: mvn clean package"
    exit 1
fi

echo "🚀 Starting Uber-Lite Lesson 01"
echo "Mode: $MODE"
echo "JAR: $JAR_FILE"
echo ""

cd "$PROJECT_DIR"
java -jar "$JAR_FILE" "$MODE"


