# Uber-Lite Lesson 10: Production Dependency Management

## Overview
Demonstrates production-grade dependency management for Kafka Streams apps using native libraries (RocksDB, H3).

## Key Concepts
- Strict dependency version alignment
- Native library loading via JNI
- Platform-specific binary validation
- Conflict resolution strategies

## Build
```bash
./gradlew build
# or: ./build.sh
```

## Run Validation
```bash
./gradlew run
# or: ./start.sh
```

## Dashboard
```bash
./start-dashboard.sh
# Open http://localhost:8081/dashboard
```

## Tests
```bash
./gradlew test
# or: ./test.sh
```

## Cleanup
```bash
./cleanup.sh
```

## Check Dependencies
```bash
./gradlew dependencies --configuration runtimeClasspath
```

## Expected Output
```
=== Dependency Validation Started ===

Platform: linux / amd64
✓ Platform validation passed

Validating RocksDB...
RocksDB version: 9.7.4
✓ RocksDB functional test passed

Validating H3...
H3 cell for NYC (40.7128, -74.0060) at res 9: 892a1072b43ffff
✓ H3 functional test passed

=== All Validations Passed ===
```
