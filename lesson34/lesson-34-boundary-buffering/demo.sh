#!/usr/bin/env bash
set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"
echo "==> BoundaryBufferingApp runs its own load generator. If app is running, metrics already update."
echo "==> Running standalone LoadGenerator (optional extra load)..."
MVN="mvn"; [ -x "$DIR/mvnw" ] && MVN="$DIR/mvnw"
"$MVN" exec:java -q -Dexec.mainClass="com.uberlite.boundary.LoadGenerator" -Dexec.args="localhost:9092 100 20" || true
echo "==> Refresh dashboard: http://localhost:8080/"
