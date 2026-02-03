#!/bin/bash
set -e
cd "$(dirname "$0")"
echo "🧪 Verifying Lesson 13: Kafka Producer"
echo ""

# Check metrics.json exists and has non-zero records
if [ -f metrics.json ]; then
  TOTAL=$(grep -oE '"total_records"[[:space:]]*:[[:space:]]*[0-9]+' metrics.json | grep -oE '[0-9]+' | head -1 || echo "0")
  if [ "$TOTAL" -gt 0 ]; then
    echo "✅ metrics.json: total_records=$TOTAL"
  else
    echo "⚠️  metrics.json exists but total_records is 0. Run: mvn exec:java"
    exit 1
  fi
else
  echo "❌ metrics.json not found. Run: mvn clean compile exec:java"
  exit 1
fi

# Check partition distribution
if grep -q '"partition_distribution"' metrics.json; then
  echo "✅ partition_distribution present"
fi

echo ""
echo "✅ Verification passed"
