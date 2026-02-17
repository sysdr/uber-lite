#!/usr/bin/env bash
# Build Firehose Capstone (Maven only). Uses full path; does not start Docker or run the app.
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

echo "==> Build OK: $JAR_PATH"
