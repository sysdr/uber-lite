#!/bin/bash
set -e

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_DIR"

echo "=== Starting Continuous Data Producer ==="
echo "This will keep producing driver locations to keep metrics updating"
echo "Press Ctrl+C to stop"
echo ""

# Check if Kafka is running
if ! docker ps | grep -q "kafka"; then
    echo "❌ Kafka is not running. Please start it first with ./start.sh"
    exit 1
fi

# Check if Streams apps are running
if ! pgrep -f "partition-skew-geohash" > /dev/null; then
    echo "⚠️  Kafka Streams apps not running. Starting them..."
    CLASSPATH=$(mvn dependency:build-classpath -q -DincludeScope=compile -Dmdep.outputFile=/dev/stdout)
    
    # Start geohash streams app
    nohup java -cp "target/classes:$CLASSPATH" -Dstreams.type=geohash com.uberlite.geohash.demo.StreamsAppRunner > /tmp/streams-geohash.log 2>&1 &
    
    # Start h3 streams app  
    nohup java -cp "target/classes:$CLASSPATH" -Dstreams.type=h3 com.uberlite.geohash.demo.StreamsAppRunner > /tmp/streams-h3.log 2>&1 &
    
    echo "Waiting for Streams apps to start..."
    sleep 5
fi

# Cities for generating locations
CITIES=("Singapore:1.3:103.8" "Mumbai:19.0:72.8" "New York:40.7:-74.0" "London:51.5:-0.1" "Oslo:59.9:10.7" "Helsinki:60.2:24.9" "Reykjavik:64.1:-21.9" "Anchorage:61.2:-149.9")

KAFKA_CONTAINER=$(docker ps | grep kafka | awk '{print $1}' | head -1)
COUNTER=0

while true; do
    # Pick a random city
    CITY_DATA=${CITIES[$RANDOM % ${#CITIES[@]}]}
    IFS=':' read -r CITY_NAME LAT LON <<< "$CITY_DATA"
    
    # Generate random offset
    LAT_OFFSET=$(echo "scale=4; ($RANDOM % 100 - 50) / 1000.0" | bc)
    LON_OFFSET=$(echo "scale=4; ($RANDOM % 100 - 50) / 1000.0" | bc)
    
    FINAL_LAT=$(echo "scale=4; $LAT + $LAT_OFFSET" | bc)
    FINAL_LON=$(echo "scale=4; $LON + $LON_OFFSET" | bc)
    
    DRIVER_ID="driver_continuous_$COUNTER"
    TIMESTAMP=$(date +%s)000
    
    MESSAGE="{\"driverId\":\"$DRIVER_ID\",\"lat\":$FINAL_LAT,\"lon\":$FINAL_LON,\"timestamp\":$TIMESTAMP,\"cityName\":\"$CITY_NAME\"}"
    
    # Send to both topics
    docker exec "$KAFKA_CONTAINER" sh -c \
      "echo '$MESSAGE' | kafka-console-producer --bootstrap-server localhost:9092 --topic driver-locations-geohash" 2>/dev/null
    
    docker exec "$KAFKA_CONTAINER" sh -c \
      "echo '$MESSAGE' | kafka-console-producer --bootstrap-server localhost:9092 --topic driver-locations-h3" 2>/dev/null
    
    COUNTER=$((COUNTER + 1))
    
    if [ $((COUNTER % 10)) -eq 0 ]; then
        echo "Produced $COUNTER messages..."
    fi
    
    # Wait 2 seconds between messages
    sleep 2
done

