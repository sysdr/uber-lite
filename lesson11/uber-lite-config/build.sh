#!/bin/bash
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
echo "Building Lesson 11 - Config Architecture"
if command -v mvn &>/dev/null && command -v java &>/dev/null; then
  mvn clean package -DskipTests
else
  echo "Using Maven Docker image..."
  docker run --rm -v "$SCRIPT_DIR":/workspace -w /workspace maven:3.9-eclipse-temurin-21 mvn clean package -DskipTests
fi
echo "Build complete."
