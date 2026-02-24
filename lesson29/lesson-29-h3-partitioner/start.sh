#!/usr/bin/env bash
set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"
if command -v curl &>/dev/null && curl -sf http://localhost:8080/health &>/dev/null 2>&1; then
  echo "==> App already running on http://localhost:8080 (health OK). Skip start to avoid duplicate."
  echo "  Dashboard: http://localhost:8080/"
  exit 0
fi
echo "==> Stopping any existing dashboard (no duplicate)..."
pkill -f "com.uberlite.lesson29.MetricsDashboardApp" 2>/dev/null || true
sleep 2
echo "==> Starting Docker (Zookeeper + Kafka)..."
docker compose up -d
echo "==> Waiting for Kafka..."
sleep 15
MVN="mvn"; [ -x "$DIR/mvnw" ] && MVN="$DIR/mvnw"
echo "==> Compiling and building classpath..."
"$MVN" compile -q
"$MVN" dependency:copy-dependencies -DincludeScope=runtime -DoutputDirectory=target/lib -q
CP="$DIR/target/classes:$DIR/target/lib/*"
echo "==> Starting MetricsDashboardApp (background)..."
java -cp "$CP" com.uberlite.lesson29.MetricsDashboardApp &
sleep 3
echo "  Dashboard: http://localhost:8080/  Demo: $DIR/demo.sh or click Run demo"
