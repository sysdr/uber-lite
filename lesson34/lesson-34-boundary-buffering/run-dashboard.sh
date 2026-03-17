#!/usr/bin/env bash
# Start only the dashboard (port 8080). Main app must be running for non-zero metrics.
set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"
if command -v curl &>/dev/null && curl -sf http://127.0.0.1:8080/health &>/dev/null; then
  echo "Dashboard already running: http://127.0.0.1:8080/  (or http://localhost:8080/)"
  exit 0
fi
echo "==> Building classpath..."
MVN="mvn"; [ -x "$DIR/mvnw" ] && MVN="$DIR/mvnw"
"$MVN" compile -q
"$MVN" dependency:copy-dependencies -DincludeScope=runtime -DoutputDirectory=target/lib -q 2>/dev/null || true
CP="$DIR/target/classes:$DIR/target/lib/*"
echo "==> Starting dashboard on http://0.0.0.0:8080 (use http://localhost:8080 in browser)"
exec java -cp "$CP" com.uberlite.boundary.MetricsDashboardApp
