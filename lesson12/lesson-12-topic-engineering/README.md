# Lesson 12: Topic Engineering

## Quick Start
```bash
# 1. Initialize topics
./topics_init.sh

# 2. Verify configuration
./verify.sh
```

## What This Lesson Teaches

- **Immutable scaling contracts**: Why partition count must be correct from day one
- **Capacity planning**: Formula for calculating partition counts based on throughput
- **Replication topology**: Why RF=3 with min.insync.replicas=2 ensures durability
- **Topic configuration**: retention.ms, segment.ms, compression.type tuning

## Topic Specifications

| Topic | Partitions | RF | Throughput | Retention |
|-------|-----------|----|-----------:|-----------|
| driver-locations | 60 | 3 | 100K msgs/sec | 24 hours |
| rider-requests | 12 | 3 | 167 msgs/sec | 1 hour |
| ride-matches | 12 | 3 | 500 msgs/sec | 1 hour |

## Verification Checks

The verify.sh script validates:
1. ✓ Topic metadata (partition count, replication factor)
2. ✓ ISR health (all replicas in-sync)
3. ✓ Partition balance (skew < 10% under test load)
4. ✓ Producer throughput (can sustain 1000 msgs without errors)
