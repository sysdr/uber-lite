#!/bin/bash
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
echo "Running Lesson 19 tests"
REQUIRED=(pom.xml docker-compose.yml start.sh create-topics.sh dashboard_server.py start-dashboard.sh demo.sh test.sh)
for f in "${REQUIRED[@]}"; do [ -f "$f" ] || { echo "Missing: $f"; exit 1; }; done
echo "All required files exist"
mvn clean compile -q && echo "Compilation OK" || exit 1
DASH=$(pgrep -f "dashboard_server.py" 2>/dev/null | wc -l)
[ "$DASH" -le 1 ] || echo "Warning: $DASH dashboard processes (pkill -f dashboard_server.py)"
echo "Tests complete"
