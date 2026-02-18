#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$SCRIPT_DIR/lesson-26-data-locality"
MVNW="$PROJECT_DIR/mvnw"

if [ ! -x "$MVNW" ]; then
  echo "Project not built. Run ./build.sh first."
  exit 1
fi

cd "$PROJECT_DIR"
echo "==> Running DataLocalitySimulator (driver + rider events)..."
"$MVNW" exec:java@simulator
