#!/usr/bin/env bash
set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"
MVN="mvn"; [ -x "$DIR/mvnw" ] && MVN="$DIR/mvnw"
echo "==> Running PartitionerDemo (driver location events)..."
"$MVN" exec:java -q -Dexec.mainClass="com.uberlite.lesson29.PartitionerDemo"
echo "==> Demo done. Refresh dashboard: http://localhost:8080/"
