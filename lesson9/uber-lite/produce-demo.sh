#!/bin/bash
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if ! docker ps --format '{{.Names}}' | grep -q 'uber-lite-kafka-1'; then
  echo "❌ Kafka not running. Run ./start.sh first."
  exit 1
fi

echo "Producing driver location events..."
# Use kafka-console-producer for fast demo (DriverLocation JSON format)
{
  for i in $(seq 1 50); do
    TS=$(($(date +%s) * 1000))
    echo "{\"driver_id\":\"driver-$i\",\"h3_index\":89283082837000000,\"latitude\":40.7$i,\"longitude\":-73.9$i,\"timestamp\":$TS}"
  done
} | docker exec -i uber-lite-kafka-1 kafka-console-producer --bootstrap-server kafka-1:29092,kafka-2:29092,kafka-3:29092 --topic driver-locations
echo "Done. Dashboard will show non-zero counts (refresh in browser or wait ~5s)."
