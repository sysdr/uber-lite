# Lesson 18: H3-Aware Driver Model

## Quick Start

```bash
# Start Kafka cluster
docker-compose up -d

# Wait for Kafka to initialize
sleep 30

# Build and run the simulator
mvn clean package
java -jar target/driver-model-1.0.0.jar

# In another terminal, verify
./verify.sh
```

## What This Demonstrates

- H3-native driver state modeling
- Spatial co-location through H3-based partitioning
- Realistic movement simulation with heading and speed
- Production-grade Kafka producer configuration

## Key Components

- `Driver.java`: H3-aware driver record with validation
- `H3CellPartitioner.java`: Custom partitioner for spatial locality
- `DriverSimulator.java`: Main application simulating 1000 drivers
- `docker-compose.yml`: Kafka + ZooKeeper + Kafka UI
- `verify.sh`: Automated verification script

## Metrics to Monitor

- Partition skew (target: < 1.15)
- Cell transition rate (target: < 0.05/sec/driver)
- Message throughput (expect: ~500 msg/sec)
- RocksDB write amplification (target: < 3.0)

## Access Points

- Dashboard: http://localhost:8082/dashboard
- Kafka UI: http://localhost:8081
- Kafka Broker: localhost:9092
- ZooKeeper: localhost:2181

## Scripts

- `./start.sh` - Start Docker cluster and build
- `./start-dashboard.sh` - Start metrics dashboard
- `./demo.sh` - Run driver simulator for 45 seconds
- `./verify.sh` - Verify topic and messages
- `./cleanup.sh` - Stop dashboard and Docker
