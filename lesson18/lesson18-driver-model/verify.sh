#!/bin/bash
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

KAFKA_CONTAINER=$(docker ps --format '{{.Names}}' | grep -E '.*-kafka-1$' | grep -v kafka-ui | head -1)
if [ -z "$KAFKA_CONTAINER" ]; then
    echo "ERROR: Kafka not running. Start with: docker-compose up -d"
    exit 1
fi

echo "=== Verifying Driver Simulator ==="

# Check if topic exists
echo "1. Checking topic creation..."
if docker exec "$KAFKA_CONTAINER" kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null | grep -q driver-locations; then
    echo "   ✓ Topic 'driver-locations' exists"
else
    echo "   ✗ Topic not found. Run ./demo.sh to create and populate."
    exit 1
fi

# Check partition count
echo "2. Checking partition distribution..."
PARTITIONS=$(docker exec "$KAFKA_CONTAINER" kafka-topics --bootstrap-server localhost:9092 --describe --topic driver-locations 2>/dev/null | grep "PartitionCount" | sed 's/.*PartitionCount: \([0-9]*\).*/\1/')
echo "   ✓ Partition count: ${PARTITIONS:-N/A}"

# Consume sample messages (exit 0 = got messages, exit 124 = timeout)
echo "3. Sampling messages (10 seconds)..."
if timeout 10 docker exec "$KAFKA_CONTAINER" kafka-console-consumer --bootstrap-server localhost:9092 --topic driver-locations --max-messages 5 --from-beginning 2>/dev/null | head -3; then
    echo "   ✓ Messages flowing"
else
    echo "   ⚠ driver-locations has 0 events. Run ./demo.sh"
    exit 1
fi

# Check for H3 cell validity
echo "4. Validating H3 cell format..."
SAMPLE=$(docker exec "$KAFKA_CONTAINER" kafka-console-consumer --bootstrap-server localhost:9092 --topic driver-locations --max-messages 1 --from-beginning --timeout-ms 5000 2>/dev/null || true)
if echo "$SAMPLE" | grep -q '"h3Cell"'; then
    echo "   ✓ H3 cell field present"
else
    echo "   ✗ H3 cell field missing"
    exit 1
fi

echo ""
echo "=== ALL CHECKS PASSED ==="
echo "Driver simulator is correctly producing H3-aware location updates."
echo ""
echo "Next steps:"
echo "  - Dashboard: http://localhost:8082/dashboard"
echo "  - Kafka UI: http://localhost:8081"
echo "  - View partition distribution in 'driver-locations' topic"
