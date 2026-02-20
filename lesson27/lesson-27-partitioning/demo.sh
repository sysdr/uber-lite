#!/usr/bin/env bash
set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"
MVN="mvn"; [ -x "$DIR/mvnw" ] && MVN="$DIR/mvnw"
echo "==> Running rider request demo (45s) to generate matches..."
"$MVN" exec:java -q -Dexec.mainClass=com.uberlite.lesson27.SimulatedRiderProducer
echo "==> Demo done. Refresh dashboard: http://localhost:8080/dashboard"
