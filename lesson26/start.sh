#!/usr/bin/env bash
set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"
MVNW="$DIR/mvnw"
[ -x "$MVNW" ] || { echo "Missing or not executable: $MVNW"; exit 1; }
# Avoid duplicate: app binds to 8080
if command -v curl &>/dev/null && curl -sf http://localhost:8080/health &>/dev/null; then
  echo "==> App already running on http://localhost:8080 (health OK)"
  echo "  Dashboard: http://localhost:8080/ or http://localhost:8080/dashboard"
  echo "  Metrics: curl http://localhost:8080/metrics"
  echo "  Verify: $DIR/verify.sh"
  exit 0
fi
echo "==> Ensuring Kafka topics exist..."
docker exec broker1-lesson26 kafka-topics --list --bootstrap-server broker-1:29092 2>/dev/null | grep -q driver-locations || {
  docker exec broker1-lesson26 bash -c '
    for t in driver-locations rider-requests match-results; do
      kafka-topics --create --if-not-exists --bootstrap-server broker-1:29092 --topic "$t" --partitions 12 --replication-factor 3 --config retention.ms=3600000
    done
  '
}
echo "==> Starting LocalityMatchingApp (background)..."
"$MVNW" exec:java -Dexec.mainClass=com.uberlite.locality.LocalityMatchingApp &
APP_PID=$!
echo "  PID: $APP_PID (wait ~25s for rebalance, then run demo)"
echo "  Demo: $MVNW exec:java@simulator"
echo "  Dashboard: http://localhost:8080/ or http://localhost:8080/dashboard"
echo "  Metrics: curl http://localhost:8080/metrics"
echo "  Verify: $DIR/verify.sh"
