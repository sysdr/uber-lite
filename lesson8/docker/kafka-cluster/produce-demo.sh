#!/bin/bash
# produce-demo.sh - Send sample messages so dashboard/verify show non-zero data

set -e
echo "Producing sample messages to driver-locations, rider-locations, and ride-matches..."
for i in $(seq 1 20); do
  echo "{\"driverId\":\"d-$i\",\"lat\":37.7$i,\"lng\":-122.4$i,\"ts\":$(date +%s)000}" | \
    docker exec -i kafka-1 kafka-console-producer --bootstrap-server localhost:9092 --topic driver-locations 2>/dev/null || true
  echo "{\"riderId\":\"r-$i\",\"lat\":37.8$i,\"lng\":-122.3$i,\"ts\":$(date +%s)000}" | \
    docker exec -i kafka-1 kafka-console-producer --bootstrap-server localhost:9092 --topic rider-locations 2>/dev/null || true
done
for i in $(seq 1 20); do
  echo "{\"rideId\":\"ride-$i\",\"driverId\":\"d-$i\",\"riderId\":\"r-$i\",\"ts\":$(date +%s)000}" | \
    docker exec -i kafka-1 kafka-console-producer --bootstrap-server localhost:9092 --topic ride-matches 2>/dev/null || true
done
echo "Done. Dashboard will show non-zero counts (refresh in browser or wait ~5s for auto-refresh)."
