#!/usr/bin/env bash
set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"
if command -v curl &>/dev/null && curl -sf http://localhost:8080/health &>/dev/null 2>&1; then
  echo "==> Dashboard already running on http://localhost:8080. Skip start to avoid duplicate."
  echo "  Dashboard: http://localhost:8080/"
  exit 0
fi
echo "==> Stopping any existing lesson34 processes (no duplicate)..."
pkill -f "com.uberlite.boundary.MetricsDashboardApp" 2>/dev/null || true
pkill -f "com.uberlite.boundary.BoundaryBufferingApp" 2>/dev/null || true
sleep 2
echo "==> Starting Docker (Zookeeper + Kafka)..."
docker compose up -d || true
echo "==> Waiting for Kafka..."
for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20; do
  if command -v nc &>/dev/null && nc -z localhost 9092 2>/dev/null; then
    echo "  Kafka port 9092 is open."
    break
  fi
  [ "$i" -eq 20 ] && { echo "WARN: Kafka not detected on 9092 after 40s. Proceeding anyway."; }
  sleep 2
done
echo "==> Waiting for Kafka topics (driver-location-raw)..."
for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20; do
  if docker exec lesson-34-boundary-buffering-kafka-1 kafka-topics --list --bootstrap-server localhost:9092 2>/dev/null | grep -q driver-location-raw; then
    echo "  Topics ready."
    break
  fi
  [ "$i" -eq 20 ] && echo "WARN: Topic driver-location-raw not found. kafka-init may still be running."
  sleep 2
done
sleep 5
MVN="mvn"; [ -x "$DIR/mvnw" ] && MVN="$DIR/mvnw"
echo "==> Compiling..."
"$MVN" compile -q
"$MVN" dependency:copy-dependencies -DincludeScope=runtime -DoutputDirectory=target/lib -q 2>/dev/null || true
CP="$DIR/target/classes:$DIR/target/lib/*"
echo "==> Starting BoundaryBufferingApp (background, metrics on :7072)..."
nohup java -cp "$CP" com.uberlite.boundary.BoundaryBufferingApp >> "$DIR/app.log" 2>&1 &
APP_PID=$!
echo "  BoundaryBufferingApp PID: $APP_PID (log: $DIR/app.log)"
echo "==> Waiting for metrics server :7072 (up to 90s)..."
for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30; do
  if curl -sf http://localhost:7072/health &>/dev/null; then
    echo "  Metrics server :7072 is up."
    break
  fi
  [ "$i" -eq 30 ] && echo "WARN: :7072 not responding. Check app.log. Dashboard may show zeros until app is ready."
  sleep 3
done
echo "==> Starting MetricsDashboardApp (background, dashboard on :8080)..."
nohup java -cp "$CP" com.uberlite.boundary.MetricsDashboardApp >> "$DIR/dashboard.log" 2>&1 &
sleep 2
echo ""
echo "  Dashboard URL: http://localhost:8080/"
echo "  Raw metrics:   http://localhost:8080/raw-metrics   Health: http://localhost:8080/health"
echo "  Metrics will update within 1-2 minutes. If still zero, check: tail -f $DIR/app.log"
echo ""
