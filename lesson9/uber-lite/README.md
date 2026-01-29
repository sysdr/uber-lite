# Uber-Lite: High-Scale Geo-Spatial Matchmaking

## Quick Start

### Prerequisites
- Docker 24+ with Compose V2
- Java 21 (OpenJDK or GraalVM)
- Maven 3.9+

### Build & Run

```bash
# Build all modules
mvn clean install

# Start Kafka cluster (3 brokers)
docker-compose up -d

# Wait for cluster to stabilize
sleep 30

# Create topic with replication
docker exec uber-lite-kafka-1 kafka-topics \
  --create --bootstrap-server localhost:9092 \
  --topic driver-locations \
  --partitions 16 \
  --replication-factor 3 \
  --config min.insync.replicas=2

# Run producer (simulates 3,000 drivers/sec)
java -jar uber-lite-producer/target/uber-lite-producer-1.0.0-SNAPSHOT.jar

# Run streams app (in separate terminal)
java -jar uber-lite-streams/target/uber-lite-streams-1.0.0-SNAPSHOT.jar
```

### Verify Setup

```bash
# Check topic was created
docker exec uber-lite-kafka-1 kafka-topics --list --bootstrap-server localhost:9092

# Monitor consumer lag
docker exec uber-lite-kafka-1 kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group geo-index-app
```

## Project Structure

- `uber-lite-common`: Shared domain models (DriverLocation record)
- `uber-lite-producer`: Raw Kafka producer with tuned batching
- `uber-lite-streams`: Kafka Streams with RocksDB state stores

## Key Configuration

### Producer Tuning
- `batch.size=1MB`: Reduces network RPCs by 10x
- `linger.ms=10ms`: Allows batches to fill at 3k/sec rate
- `acks=all`: Waits for all in-sync replicas (durability)

### Kafka Cluster
- 3 brokers with RF=3
- `min.insync.replicas=2`: Survives single broker failure
- 16 partitions: Enables 16-way parallelism

## Next Steps

Lesson 10 will implement H3-based custom partitioning to co-locate drivers and riders by geospatial proximity.
