# Lesson 21: Handling Simulation Backpressure

## Quick Start

```bash
# Start Kafka
docker-compose up -d

# Wait for Kafka to be ready (30 seconds)
sleep 30

# Build the project
mvn clean package

# Run with backpressure handling (default)
java -jar target/driver-simulator-1.0.jar

# Run WITHOUT backpressure (will crash)
java -Dbackpressure.enabled=false -Xmx512m -jar target/driver-simulator-1.0.jar
```

## What to Observe

### With Backpressure Enabled
- Heap stays under 512MB
- Metrics show occasional buffer exhaustion events
- Exponential backoff prevents OOM
- All 10,000 drivers successfully send updates

### Without Backpressure (Crash Demo)
- Heap climbs rapidly to max
- OutOfMemoryError after 2-3 minutes
- Producer threads blocked in send()
- Simulation halts

## Architecture

The `BackpressureProducer` implements three layers of defense:
1. **Fail-fast config**: `max.block.ms=0` throws exception instead of blocking
2. **Adaptive throttling**: Exponential backoff with jitter on buffer exhaustion
3. **Proactive monitoring**: Checks `buffer-available-bytes` before sending

## Verification

Run `./verify.sh` to validate the implementation handles 10K drivers without OOM.
