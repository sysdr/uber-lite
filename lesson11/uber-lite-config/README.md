# Lesson 11: Configuration Architecture

## Quick Start
```bash
# 1. Start Kafka cluster
docker-compose up -d

# 2. Wait for brokers to stabilize
sleep 10

# 3. Run with dev profile
mvn spring-boot:run -Dspring-boot.run.profiles=dev

# 4. Check health endpoint
curl http://localhost:8080/health

# 5. View resolved config
curl http://localhost:8080/config
```

## Profile Testing

### Local (no Docker)
```bash
mvn spring-boot:run
# Uses localhost:9092
```

### Dev (Docker Compose)
```bash
mvn spring-boot:run -Dspring-boot.run.profiles=dev
# Uses kafka-broker-1:9092,kafka-broker-2:9092,kafka-broker-3:9092
```

### Prod Simulation
```bash
export KAFKA_BOOTSTRAP_SERVERS=prod-kafka-1:9092,prod-kafka-2:9092
mvn spring-boot:run -Dspring-boot.run.profiles=prod
# Uses prod brokers with high-throughput tuning
```

## Configuration Hierarchy

1. `application.yml` - Base defaults
2. `application-dev.yml` - Docker overrides
3. `application-prod.yml` - Production tuning
4. Environment variables - Runtime injection

## Key Metrics

- **num.stream.threads**: Should match partition count in production
- **linger.ms**: 0 for dev (immediate), 10 for prod (batching)
- **batch.size**: 16KB dev, 32KB prod
- **state-dir**: Must be on persistent volume
