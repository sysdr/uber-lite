# Lesson 39 — H3 co-partitioning (Uber-Lite)

Demonstrates H3-based custom partitioning so `driver-updates` and `rider-requests` stay co-partitioned for Kafka Streams–style joins. Includes a small HTTP dashboard on port 8080 that consumes both topics and shows live counts.

## Prerequisites

- Java 21+
- Maven 3.9+
- Docker 24+ with Compose v2

See `requirements.txt` for a short checklist (no pip install).

## Generate or refresh this project

From the parent `lesson39/` directory (where `setup.sh` lives):

```bash
cd ..
bash setup.sh
```

## Run

```bash
./start.sh      # Zookeeper + Kafka, topics, dashboard → http://localhost:8080/
./demo.sh       # Driver + rider load (~30s)
./verify.sh     # Assert dashboard metrics are non-zero
./test.sh       # Unit tests (Maven)
```

Manual validation:

```bash
mvn exec:java -Dexec.mainClass="com.uberlite.CoPartitionValidator"
```

## Stop and clean up

```bash
./cleanup.sh
```

Stops the dashboard, runs `docker compose down`, removes `target/` and logs, and prunes unused Docker objects.

## Security

There are no third-party API keys in this lesson. Kafka and the dashboard use local dev defaults only (`localhost:9092`, no SASL in the provided compose file).

## Layout

| Path | Role |
|------|------|
| `../setup.sh` | Regenerates this directory (run from parent `lesson39/`) |
| `cleanup.sh` | Stop services, remove build artifacts, Docker prune |
| `pom.xml`, `src/`, `docker-compose.yml` | App and local Kafka |
