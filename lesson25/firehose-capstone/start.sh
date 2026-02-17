#!/usr/bin/env bash
# Start Firehose Capstone: Docker, build, run app. Uses full path; checks for duplicate services.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="${SCRIPT_DIR}"
JAR_PATH="${PROJECT_DIR}/target/firehose-capstone-1.0-SNAPSHOT-jar-with-dependencies.jar"

if [[ ! -f "$PROJECT_DIR/pom.xml" ]]; then
  echo "ERROR: Project not found (no pom.xml). Run setup.sh from lesson25 first."
  exit 1
fi

if ! command -v java >/dev/null 2>&1; then
  echo "ERROR: Java not found. Install a JDK (e.g. openjdk-21) and ensure 'java' is in PATH."
  exit 1
fi

# Check for duplicate: already running jar (set FORCE_START=1 or run without TTY to skip prompt)
if pgrep -f "firehose-capstone-1.0-SNAPSHOT-jar-with-dependencies.jar" >/dev/null 2>&1; then
  echo "WARN: Firehose app appears already running (jar process found). Stop it first if you want to restart."
  if [[ "${FORCE_START:-0}" != "1" ]] && [[ -t 0 ]]; then
    read -p "Continue anyway? [y/N] " -n 1 -r; echo
    if [[ ! $REPLY =~ ^[yY]$ ]]; then exit 0; fi
  fi
fi

# Port 8080 in use by another process (could be our app or something else)
if command -v ss >/dev/null 2>&1 && ss -tlnp 2>/dev/null | grep -q ":8080 "; then
  echo "WARN: Port 8080 already in use. Another metrics server may be running."
  if [[ "${FORCE_START:-0}" != "1" ]] && [[ -t 0 ]]; then
    read -p "Continue anyway? [y/N] " -n 1 -r; echo
    if [[ ! $REPLY =~ ^[yY]$ ]]; then exit 0; fi
  fi
fi

echo "==> Starting Docker (Zookeeper + Kafka)..."
cd "$PROJECT_DIR"
docker compose up -d
echo "==> Waiting 15s for broker election..."
sleep 15

echo "==> Building with Maven..."
if [[ -x "$PROJECT_DIR/mvnw" ]]; then
  "$PROJECT_DIR/mvnw" -q -f "$PROJECT_DIR/pom.xml" clean package -DskipTests
else
  mvn -q -f "$PROJECT_DIR/pom.xml" clean package -DskipTests
fi

if [[ ! -f "$JAR_PATH" ]]; then
  echo "ERROR: JAR not found: $JAR_PATH"
  exit 1
fi

echo "==> Starting Firehose application (full path)..."
exec java -jar "$JAR_PATH"
