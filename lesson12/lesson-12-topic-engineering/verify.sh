#!/bin/bash
# verify.sh - Validate topic engineering lesson

set -e

echo "🧪 Running Topic Engineering Verification"
echo ""

# Compile Java code
cd "$(dirname "$0")"
mvn clean compile -q

# Run validator
mvn exec:java -q \
  -Dexec.mainClass="com.uberlite.topics.TopicValidator" \
  -Dexec.cleanupDaemonThreads=false
