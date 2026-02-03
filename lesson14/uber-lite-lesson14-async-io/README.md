# Lesson 14: Asynchronous I/O

## Quick Start

```bash
# Start Kafka
docker-compose up -d

# Wait for Kafka to be ready (30 seconds)
sleep 30

# Run async demo (fast - ~5000 events/sec)
mvn clean compile exec:java -Dexec.mainClass="com.uberlite.async.AsyncProducerDemo"

# Compare with sync demo (slow - ~200 events/sec)
mvn exec:java -Dexec.mainClass="com.uberlite.async.SyncProducerDemo"
```

## Key Concepts

1. **Async send**: `producer.send(record, callback)` - returns immediately
2. **RecordAccumulator**: Batches records in memory before network send
3. **Callback thread safety**: Callbacks run on I/O threads, not producer threads
4. **Virtual threads**: Enable 1000+ concurrent event generators with minimal overhead

## Kafka UI

View metrics at: http://localhost:8080

## Cleanup

```bash
docker-compose down -v
```
