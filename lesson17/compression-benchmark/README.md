# Lesson 17: Compression Benchmark

## Quick Start

```bash
# Start Kafka cluster
docker-compose up -d

# Wait for Kafka readiness
sleep 10

# Build and run benchmark
mvn clean package
java -jar target/compression-benchmark-1.0.jar
```

## Expected Output

```
Codec: none     | Compression: 1.00x | Throughput: 8,234 events/sec | Network: 2.88 MB/sec
Codec: lz4      | Compression: 3.00x | Throughput: 7,891 events/sec | Network: 0.96 MB/sec
Codec: snappy   | Compression: 2.50x | Throughput: 8,102 events/sec | Network: 1.15 MB/sec
Codec: gzip     | Compression: 6.50x | Throughput: 4,123 events/sec | Network: 0.44 MB/sec
```

## Key Observations

1. **LZ4**: Best balance - 3x compression, minimal throughput impact
2. **Snappy**: Fastest, but lower compression ratio
3. **Gzip**: Highest compression, but 50% throughput penalty
4. **None**: Baseline - 2.88 MB/sec network usage

## Verification

Run `bash verify.sh` to validate compression effectiveness.
