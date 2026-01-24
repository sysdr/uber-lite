#!/bin/bash
set -e

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_DIR"

echo "=== Verifying Partition Skew Metrics ==="
echo ""

KAFKA_CONTAINER=$(docker ps | grep kafka | awk '{print $1}' | head -1)
if [ -z "$KAFKA_CONTAINER" ]; then
    echo "❌ Kafka container not found"
    exit 1
fi

echo "Reading metrics from partition-metrics topic..."
docker exec "$KAFKA_CONTAINER" kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic partition-metrics \
    --from-beginning \
    --max-messages 50 \
    --timeout-ms 5000 2>/dev/null | while read line; do
    echo "$line" | python3 -m json.tool 2>/dev/null || echo "$line"
done

echo ""
echo "📊 Check dashboard: http://localhost:8080/dashboard"
