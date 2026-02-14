#!/usr/bin/env bash
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "Starting Lesson 24 (Stress Test)"
echo ""

if ! docker ps --format '{{.Names}}' | grep -q 'lesson24-kafka-1'; then
  echo "Starting Docker cluster..."
  (docker compose down -v 2>/dev/null || docker-compose down -v 2>/dev/null || true)
  (docker compose up -d 2>/dev/null || docker-compose up -d)
  echo "Waiting for all 3 Kafka brokers to be running..."
  for i in $(seq 1 90); do
    up=$(docker ps --format '{{.Names}}' | grep -c 'lesson24-kafka' || true)
    [ "$up" -ge 3 ] && break
    sleep 1
  done
  echo "Waiting for cluster to stabilize (20s)..."
  sleep 20
  up=$(docker ps --format '{{.Names}}' | grep -c 'lesson24-kafka' || true)
  if [ "$up" -lt 3 ]; then
    echo "Only $up Kafka broker(s) running. Check: docker ps -a && docker logs lesson24-kafka-1"
    exit 1
  fi
  echo "Cluster is up."
fi

"$SCRIPT_DIR/create-topics.sh"

echo ""
echo "Building project..."
mvn clean package -DskipTests -q
echo "Build complete."

echo ""
echo "Cluster ready. Run ./demo.sh to produce load and update dashboard metrics."
echo "Dashboard: ./start-dashboard.sh  then open http://localhost:8080/dashboard"
