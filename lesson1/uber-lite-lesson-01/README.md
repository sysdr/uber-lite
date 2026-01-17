# Lesson 01: The Monolith vs. The Event Log

## Quick Start

```bash
# Start infrastructure
docker-compose up -d

# Wait for services to be ready (30 seconds)
sleep 30

# Build project
mvn clean package

# Run comparison (both PostgreSQL and Kafka)
java -jar target/lesson-01-monolith-vs-eventlog-1.0.0.jar both

# Or run individually:
# java -jar target/lesson-01-monolith-vs-eventlog-1.0.0.jar postgres
# java -jar target/lesson-01-monolith-vs-eventlog-1.0.0.jar kafka
```

## Query Driver Location (Kafka mode)

```bash
curl http://localhost:8080/driver/driver-abc123
```

## Clean Up

```bash
docker-compose down -v
```
