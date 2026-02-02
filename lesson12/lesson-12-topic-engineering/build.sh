#!/bin/bash
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
[ -f "$SCRIPT_DIR/pom.xml" ] || { echo "❌ Project not found. Run setup.sh first."; exit 1; }
echo "🔨 Building Lesson 12 (lesson-12-topic-engineering)..."
cd "$SCRIPT_DIR"
mvn clean package -DskipTests -q
echo "✅ Build complete."
