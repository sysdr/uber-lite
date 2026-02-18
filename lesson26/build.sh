#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "==> Running setup.sh..."
bash "$SCRIPT_DIR/setup.sh"

echo ""
echo "==> Compiling project..."
cd "$SCRIPT_DIR/lesson-26-data-locality"
./mvnw compile -q

echo ""
echo "✓ Build complete."
