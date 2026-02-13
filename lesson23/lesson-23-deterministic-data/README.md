# Lesson 23: Deterministic Data Generation

## Overview
Reproducible test data generation using seeded PRNGs. Enables debugging at scale by making failures reproducible.

## Quick Start
```bash
./build.sh
./demo.sh          # short demo (3 drivers, 15 steps)
./start.sh         # longer run (10 drivers, 100 steps)
./start-dashboard.sh   # Lesson 23 dashboard: http://localhost:8083/dashboard
./test.sh
```

## Key Concepts
- **Deterministic Mode**: Same seed → same trajectory (for tests)
- **Stochastic Mode**: High-entropy seeds (for production)
- **Per-Entity Seeding**: Each driver gets unique but reproducible seed
- **Snapshot & Replay**: Capture seed to recreate exact failure conditions

## Dashboard
Run `./start-dashboard.sh` and open **http://localhost:8083/dashboard** (Lesson 23). Run `./demo.sh` or `./start.sh` to update the metrics shown.

## Verification
Run `./test.sh` and `./demo.sh` to validate tests and reproducibility.
