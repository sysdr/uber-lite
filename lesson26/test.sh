#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Ensure project is generated and compiled
if [ ! -f "$SCRIPT_DIR/lesson-26-data-locality/mvnw" ]; then
  echo "==> Project not found. Running build.sh..."
  bash "$SCRIPT_DIR/build.sh"
fi

echo "==> Running tests..."
cd "$SCRIPT_DIR/lesson-26-data-locality"
./mvnw test -q

echo ""
echo "✓ Tests passed."
