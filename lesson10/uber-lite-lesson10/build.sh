#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "🔨 Building project: uber-lite-lesson10"
echo "Project path: $SCRIPT_DIR"
echo ""

./gradlew clean build --no-daemon

echo ""
echo "✅ Build successful!"
if [ -d "$SCRIPT_DIR/build/libs" ]; then
    ls -lh "$SCRIPT_DIR/build/libs/"*.jar 2>/dev/null || true
fi
