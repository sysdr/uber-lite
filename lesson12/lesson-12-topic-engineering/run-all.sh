#!/bin/bash
# Lesson 12 run-all: Docker (L9) -> topics_init -> L9 build -> verify -> L9 test -> dashboard -> demo
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
L9="$SCRIPT_DIR/../../lesson9/uber-lite"
[ -f "$L9/docker-compose.yml" ] || { echo "❌ Lesson 9 not found: $L9"; exit 1; }
[ -f "$SCRIPT_DIR/topics_init.sh" ] || { echo "❌ Run setup.sh first."; exit 1; }

echo "📁 Lesson 12: $SCRIPT_DIR"
echo "📁 Lesson 9:  $L9"
echo ""

# 1) Start Docker only (do NOT run L9 start.sh - it creates driver-locations with 16 partitions)
if ! docker ps --format '{{.Names}}' | grep -q 'uber-lite-kafka-1'; then
  echo "⏳ Starting Kafka cluster (Docker)..."
  if ! (cd "$L9" && (docker compose up -d 2>/dev/null || docker-compose up -d 2>/dev/null)); then
    echo "❌ Docker/Kafka start failed (e.g. network or images). Start manually:"
    echo "   cd $L9 && ./start.sh"
    echo "   Then re-run: $SCRIPT_DIR/run-all.sh"
    exit 1
  fi
  echo "⏳ Waiting for Kafka (30s)..."
  sleep 30
  if ! docker ps --format '{{.Names}}' | grep -q 'uber-lite-kafka-1'; then
    echo "❌ Kafka container not up after 30s. Start manually: cd $L9 && ./start.sh ; then re-run run-all.sh"
    exit 1
  fi
fi

# 2) Lesson 12 topic init FIRST (creates 60/12/12 partitions)
echo "📋 Lesson 12 topics_init..."
bash "$SCRIPT_DIR/topics_init.sh" || exit 1

# 3) Lesson 9 create-topics (no-op; topics exist) and build
echo ""
echo "🔨 Lesson 9 build..."
(cd "$L9" && ./create-topics.sh 2>/dev/null || true)
(cd "$L9" && mvn clean package -DskipTests -q) || { echo "❌ Lesson 9 build failed"; exit 1; }

# 4) Lesson 12 verify
echo ""
echo "🧪 Lesson 12 verify..."
(cd "$SCRIPT_DIR" && bash verify.sh) || exit 1

# 5) Lesson 9 tests
echo ""
echo "🧪 Lesson 9 tests..."
(cd "$L9" && bash test.sh) || exit 1

# 6) Single dashboard (kill duplicates)
DASH=$(pgrep -f "dashboard_server.py" 2>/dev/null | wc -l)
if [ "$DASH" -gt 1 ]; then
  echo "⚠️ Stopping duplicate dashboard processes..."
  pkill -f "dashboard_server.py" 2>/dev/null || true
  sleep 2
fi
if [ "$DASH" -lt 1 ]; then
  echo "📊 Starting dashboard..."
  (cd "$SCRIPT_DIR" && bash start-dashboard.sh) &
  sleep 3
fi

# 7) Demo for non-zero dashboard metrics
echo ""
echo "🎬 Running produce-demo for non-zero metrics..."
(cd "$L9" && bash produce-demo.sh) || exit 1

echo ""
echo "✅ Lesson 12 run-all complete."
echo "   Dashboard: http://localhost:8081/dashboard (refresh to see updated values)"
