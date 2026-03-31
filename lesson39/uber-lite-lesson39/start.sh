#!/usr/bin/env bash
set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"
if command -v curl &>/dev/null && curl -sf http://localhost:8080/health &>/dev/null 2>&1; then
  echo "==> App already running on http://localhost:8080 (health OK). Skip start to avoid duplicate."
  echo "  Dashboard: http://localhost:8080/"
  exit 0
fi
echo "==> Stopping any existing Lesson 39 dashboard (no duplicate)..."
pkill -f "com.uberlite.MetricsDashboardApp" 2>/dev/null || true
sleep 2
echo "==> Starting Docker (Zookeeper + Kafka)..."
docker compose up -d
echo "==> Waiting for Kafka health..."
for i in $(seq 1 30); do
  if docker compose exec -T kafka kafka-broker-api-versions --bootstrap-server localhost:9092 &>/dev/null; then
    echo "  Kafka ready after ${i}s"
    break
  fi
  sleep 2
  if [ "$i" -eq 30 ]; then echo "FAIL: Kafka not ready"; exit 1; fi
done
MVN="mvn"; [ -x "$DIR/mvnw" ] && MVN="$DIR/mvnw"
echo "==> Compiling..."
"$MVN" compile -q
echo "==> Topic admin (create + validate co-partitioning)..."
"$MVN" -q exec:java -Dexec.mainClass="com.uberlite.TopicAdmin"
echo "==> Building runtime classpath..."
rm -rf "$DIR/target/lib"
"$MVN" dependency:copy-dependencies -DincludeScope=runtime -DoutputDirectory=target/lib -q
CP="$DIR/target/classes:$DIR/target/lib/*"
echo "==> Starting MetricsDashboardApp (background)..."
nohup java -cp "$CP" com.uberlite.MetricsDashboardApp >> "$DIR/dashboard.log" 2>&1 &
sleep 4
echo "  Dashboard: http://localhost:8080/"
echo "  Demo: $DIR/demo.sh or click Run demo on the dashboard"
