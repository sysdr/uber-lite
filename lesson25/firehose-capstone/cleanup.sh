#!/usr/bin/env bash
# Stop Firehose app, stop Docker Compose containers, and remove unused Docker resources.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "==> Stopping Firehose application..."
pkill -f firehose-capstone-1.0-SNAPSHOT-jar-with-dependencies.jar 2>/dev/null || true

echo "==> Stopping Docker Compose..."
if [[ -f "$SCRIPT_DIR/docker-compose.yml" ]]; then
  (cd "$SCRIPT_DIR" && docker compose down 2>/dev/null) || true
fi

echo "==> Removing unused Docker resources (containers, networks, images without tag)..."
docker system prune -f 2>/dev/null || true

echo "==> Cleanup done."
