#!/usr/bin/env bash
set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"
MVN="mvn"; [ -x "$DIR/mvnw" ] && MVN="$DIR/mvnw"
echo "==> Running producer demo (10k driver-location events)..."
"$MVN" exec:java -q -Dexec.mainClass=com.uberlite.L28ProducerMain
echo "==> Demo done. Refresh dashboard: http://localhost:8080/"
