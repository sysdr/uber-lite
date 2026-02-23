#!/usr/bin/env bash
set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"
MVN="mvn"; [ -x "$DIR/mvnw" ] && MVN="$DIR/mvnw"
if command -v curl &>/dev/null && curl -sf http://localhost:8080/health &>/dev/null 2>/dev/null; then
  echo "==> App already running on http://localhost:8080 (health OK)"
  echo "  Dashboard: http://localhost:8080/"
  echo "  To restart: pkill -f MetricsDashboardApp; sleep 2; $DIR/start.sh"
  exit 0
fi
echo "==> Starting Docker (Zookeeper + Kafka)..."
docker compose up -d
echo "==> Waiting for Kafka to be ready..."
sleep 15
echo "==> Compiling and building classpath..."
"$MVN" compile -q
"$MVN" dependency:copy-dependencies -DincludeScope=runtime -DoutputDirectory=target/lib -q
CP="$DIR/target/classes:$DIR/target/lib/*"
echo "==> Starting MetricsDashboardApp (background, full classpath for real-time metrics)..."
java -cp "$CP" com.uberlite.MetricsDashboardApp &
echo "  Wait ~5s then: Dashboard http://localhost:8080/   Demo: $DIR/demo.sh or click Run demo"
