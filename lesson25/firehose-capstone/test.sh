#!/usr/bin/env bash
# Run tests: verify metrics endpoint and that dashboard values update (non-zero after demo).
# Prerequisite: Firehose app must be running (e.g. ./start.sh) and warmed up ~15s.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
exec ./verify.sh
