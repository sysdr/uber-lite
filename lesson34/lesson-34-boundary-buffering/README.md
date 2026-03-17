# Lesson 34: Boundary Buffering

Kafka Streams project for H3-based boundary multicast and deduplication of driver location events.

## Requirements

- **Java 21**
- **Maven 3.8+**
- **Docker & Docker Compose** (for Kafka and Zookeeper)

See `requirements.txt` for a short list.

## Quick Start

```bash
./start.sh
```

Then open the dashboard: **http://localhost:8080/**

## Scripts

| Script        | Description                                              |
|---------------|----------------------------------------------------------|
| `./start.sh`  | Start Docker (Kafka/Zookeeper), BoundaryBufferingApp, and dashboard |
| `./restart.sh`| Stop apps, then run `start.sh`                           |
| `./cleanup.sh`| Stop app processes and Docker containers; prune unused resources |
| `./demo.sh`   | Optional: run standalone LoadGenerator                   |
| `./verify.sh` | Check dashboard health and non-zero metrics             |
| `./test.sh`   | Run Maven tests                                          |

## Cleanup

To stop all services and remove Docker resources:

```bash
./cleanup.sh
```

## Project layout

- `src/main/java/com/uberlite/boundary/` — application and dashboard code
- `src/main/resources/` — logback.xml, dashboard.html
- `docker-compose.yml` — Zookeeper and Kafka
- `pom.xml` — Maven build and dependencies

## Endpoints

- **Dashboard:** http://localhost:8080/
- **Raw metrics:** http://localhost:8080/raw-metrics
- **Health:** http://localhost:8080/health
- **Metrics source (app):** http://localhost:7072/metrics (when app is running)
