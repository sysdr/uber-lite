# Lesson 13: Kafka Producer API

## Quick Start

```bash
# Start Kafka cluster
docker compose up -d

# Wait for cluster to be ready (30 seconds)
sleep 30

# Run producer (sends 10k location updates)
mvn clean compile exec:java

# View metrics
cat metrics.json

# Verify results
./verify.sh
```

## Key Concepts Demonstrated

1. **Batching**: `linger.ms=5` + `batch.size=32KB` achieves 200x throughput vs sync sends
2. **H3 Partitioning**: Uniform spatial distribution, no hot spots
3. **Binary Serialization**: 40 bytes vs 80+ for JSON
4. **Virtual Thread Callbacks**: Handle 10k+ concurrent async sends
5. **Idempotent Producer**: `enable.idempotence=true` prevents duplicates

## Expected Performance

- Throughput: >3,000 events/sec
- Partition CV: <0.15 (uniform distribution)
- P99 Latency: <50ms
- Batch size: 20-30KB average

## Architecture

```
DriverLocation → H3 Index → Partitioner → RecordAccumulator → Batch → Sender Thread → Kafka
```
