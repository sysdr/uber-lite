# Lesson 16: Artificial Latency (linger.ms)

## Quick Start

```bash
# Start Kafka cluster
docker-compose up -d

# Wait for Kafka to be ready (10 seconds)
sleep 10

# Build project
mvn clean package

# Run baseline (no batching)
java --enable-preview -jar target/lesson16-linger-ms-1.0.0.jar baseline

# Run optimized (10ms batching)
java --enable-preview -jar target/lesson16-linger-ms-1.0.0.jar optimized

# Run extreme (50ms batching)
java --enable-preview -jar target/lesson16-linger-ms-1.0.0.jar extreme
```

## Expected Results

| Mode | Duration | Throughput | Batch Size | Request Rate |
|------|----------|------------|------------|--------------|
| Baseline | ~2800ms | ~1070/sec | ~500 bytes | ~2900/sec |
| Optimized | ~1000ms | ~3000/sec | ~11KB | ~30/sec |
| Extreme | ~800ms | ~3750/sec | ~15KB | ~8/sec |

## Key Observations

1. **Throughput scales with batching**: 3x improvement from baseline to optimized
2. **Request reduction is dramatic**: 99% fewer network requests
3. **Diminishing returns**: 50ms linger only adds 25% vs 10ms
4. **Latency trade-off**: Acceptable for GPS tracking, not for trades

## Cleanup

```bash
docker-compose down -v
```
