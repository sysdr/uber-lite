#!/bin/bash
# start.sh - Start Lesson 8 Kafka cluster (Docker)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ ! -f "$SCRIPT_DIR/cluster-ops.sh" ]; then
  echo "Cluster not generated. Run ./setup.sh first."
  exit 1
fi

cd "$SCRIPT_DIR"
exec ./cluster-ops.sh start
