# Lesson 6: H3 Spatial Index

## Architecture

This implementation demonstrates H3 hexagonal spatial indexing for high-velocity driver location updates.

### Components

1. **H3SpatialIndex**: RocksDB-backed spatial index using H3 cells as key prefixes
2. **DriverLocationConsumer**: Ingests location updates from Kafka
3. **MatchRequestHandler**: Processes rider requests using K-ring prefix scans

### Key Concepts

- H3 Resolution 9 (~0.1 km² hexagons)
- K=2 ring search (37 hexagons)
- RocksDB prefix bloom filters
- O(K² × M × log N) complexity vs O(N) full scan

## Running
```bash
# Start Kafka
docker-compose up -d

# Build
mvn clean package

# Run
java --enable-preview -jar target/h3-spatial-index-1.0-shaded.jar
```

## Topics

- `driver-locations`: DriverLocationUpdate events
- `rider-match-requests`: RiderMatchRequest events
- `match-results`: MatchResult events
