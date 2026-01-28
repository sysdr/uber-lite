#!/bin/bash
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
if ! docker ps --format '{{.Names}}' | grep -q '^kafka-1$'; then
  echo "Cluster not running. Start: ./start.sh or ./cluster-ops.sh start"
  exit 1
fi
exec "$SCRIPT_DIR/verify.sh"
