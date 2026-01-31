#!/bin/bash
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
echo "Starting Lesson 11 - Config Architecture"

# Our cluster (lesson11)
if docker ps --format '{{.Names}}' 2>/dev/null | grep -q 'kafka-broker-1'; then
  echo "Lesson 11 Kafka cluster already running."
elif command -v ss &>/dev/null && (ss -tlnp 2>/dev/null | grep -q ':2181 ' || ss -tlnp 2>/dev/null | grep -q ':9092 '); then
  echo "Port 2181 or 9092 already in use (another Kafka cluster may be running)."
  echo "You can run the app against it: export KAFKA_BOOTSTRAP_SERVERS=localhost:9092"
else
  echo "Starting Docker cluster..."
  docker compose up -d 2>/dev/null || docker-compose up -d
  echo "Waiting for brokers (15s)..."
  sleep 15
fi

echo ""
echo "Run app: mvn spring-boot:run -Dspring-boot.run.profiles=dev"
echo "With existing Kafka: KAFKA_BOOTSTRAP_SERVERS=localhost:9092 mvn spring-boot:run -Dspring-boot.run.profiles=dev"
echo "If 8080 in use: add -Dspring-boot.run.arguments=\"--server.port=8081\" then curl http://localhost:8081/health"
echo "Then: curl http://localhost:8080/health"
