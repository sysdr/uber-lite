#!/usr/bin/env bash
set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"
MVN="mvn"; [ -x "$DIR/mvnw" ] && MVN="$DIR/mvnw"
echo "==> DriverProducer + RiderProducer (parallel, ~30s each)..."
"$MVN" -q exec:java -Dexec.mainClass="com.uberlite.DriverProducer" &
PID1=$!
"$MVN" -q exec:java -Dexec.mainClass="com.uberlite.RiderProducer" &
PID2=$!
wait $PID1 $PID2
echo "==> Demo done. Dashboard: http://localhost:8080/"
