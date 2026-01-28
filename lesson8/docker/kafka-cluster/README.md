# Uber-Lite Kafka Cluster (Lesson 8)

Multi-broker Kafka cluster with replication, Schema Registry, and Docker networking.

## Quick Start
```bash
# Start cluster
./cluster-ops.sh start

# Check status
./cluster-ops.sh status

# View logs
./cluster-ops.sh logs kafka-1

# List topics
./cluster-ops.sh topics

# Stop cluster
./cluster-ops.sh stop
```

## Architecture

- **3 Kafka Brokers**: Replication factor 3, min.insync.replicas 2
- **1 ZooKeeper**: Cluster coordination
- **Schema Registry**: Avro schema management

## Ports

- ZooKeeper: 2181
- Kafka Broker 1: 19092 (JMX: 9999)
- Kafka Broker 2: 19093 (JMX: 10000)
- Kafka Broker 3: 19094 (JMX: 10001)
- Schema Registry: 8081

## Bootstrap Servers

From host machine:
```
localhost:19092,localhost:19093,localhost:19094
```

From Docker network:
```
kafka-1:9092,kafka-2:9092,kafka-3:9092
```

## Monitoring

JMX exposed on ports 9999-10001. Use JConsole:
```bash
jconsole localhost:9999
```

Key metrics:
- `kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions`
- `kafka.server:type=ReplicaManager,name=IsrShrinksPerSec`
- `kafka.network:type=RequestMetrics,name=TotalTimeMs`
