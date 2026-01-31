#!/bin/bash
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
echo "Running Lesson 11 tests"
REQUIRED=(pom.xml docker-compose.yml start.sh src/main/resources/application.yml src/main/java/com/uberlite/config/ConfigDemoApplication.java src/main/java/com/uberlite/config/HealthController.java)
MISSING=()
for f in "${REQUIRED[@]}"; do
  [ -e "$f" ] || MISSING+=("$f")
done
if [ ${#MISSING[@]} -gt 0 ]; then
  echo "Missing: ${MISSING[*]}"; exit 1
fi
echo "All required files exist"
mvn -q compile 2>/dev/null && echo "Compile OK" || { echo "Compile failed"; exit 1; }
echo "Tests complete"
