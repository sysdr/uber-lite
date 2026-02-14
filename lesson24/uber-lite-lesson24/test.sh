#!/usr/bin/env bash
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "Running Lesson 24 tests"
echo ""

REQUIRED=("pom.xml" "docker-compose.yml" "create-topics.sh" "dashboard_server.py" "start-dashboard.sh" "start.sh" "demo.sh" "test.sh")
MISSING=()
for f in "${REQUIRED[@]}"; do
  [ -f "$f" ] || MISSING+=("$f")
done
if [ ${#MISSING[@]} -gt 0 ]; then
  echo "Missing: ${MISSING[*]}"
  exit 1
fi
echo "All required files exist"

mvn clean compile -q 2>/dev/null && echo "Compilation OK" || echo "Compilation failed"

DASH=$(pgrep -f "dashboard_server.py" 2>/dev/null | wc -l)
if [ "$DASH" -gt 1 ]; then echo "Warning: $DASH dashboard processes (should be 0 or 1)"; else echo "Dashboard processes: $DASH"; fi

echo ""
echo "Tests complete"
