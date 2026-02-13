#!/bin/bash
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
echo "🧪 Running Lesson 23 tests"
echo ""
mvn test
echo ""
echo "✅ Tests complete"
