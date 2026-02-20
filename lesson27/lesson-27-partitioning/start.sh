#!/usr/bin/env bash
set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"
MVN="mvn"; [ -x "$DIR/mvnw" ] && MVN="$DIR/mvnw"
if command -v curl &>/dev/null && curl -sf http://localhost:8080/health &>/dev/null 2>/dev/null; then
  echo "==> App already running on http://localhost:8080 (health OK)"
  echo "  Dashboard: http://localhost:8080/ or http://localhost:8080/dashboard"
  echo "  Verify: $DIR/verify.sh"
  exit 0
fi
echo "==> Starting Docker (Zookeeper + Kafka)..."
docker compose up -d
echo "==> Waiting for Kafka to be ready..."
sleep 15
echo "==> Compiling..."
"$MVN" compile -q
echo "==> Creating topics (TopicBootstrap)..."
"$MVN" exec:java -q -Dexec.mainClass=com.uberlite.lesson27.TopicBootstrap
echo "==> Starting PartitionedStreamsApp (background)..."
"$MVN" exec:java -Dexec.mainClass=com.uberlite.lesson27.PartitionedStreamsApp &
APP_PID=$!
echo "  PID: $APP_PID (wait ~20s for rebalance, then run demo)"
echo "  Dashboard: http://localhost:8080/ or http://localhost:8080/dashboard"
echo "  Demo: $DIR/demo.sh"
echo "  Verify: $DIR/verify.sh"
