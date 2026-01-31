#!/usr/bin/env bash
# verify.sh - Lesson 11 acceptance: profile, bootstrap, state-dir, num.stream.threads
# Usage: ./verify.sh [BASE_URL]
#   BASE_URL default: http://localhost:8080
#   For state-dir check: run from app working directory, or set STATE_DIR_OVERRIDE to absolute path.
set -euo pipefail

BASE_URL="${1:-http://localhost:8080}"
HEALTH_URL="${BASE_URL}/health"
CONFIG_URL="${BASE_URL}/config"

err() { echo "ERROR: $*" >&2; exit 1; }

# --- 1) Fetch health and config (fail fast if app not reachable) ---
HEALTH_JSON=$(curl -sf "$HEALTH_URL") || err "Could not reach $HEALTH_URL (app running?)"
CONFIG_JSON=$(curl -sf "$CONFIG_URL") || err "Could not reach $CONFIG_URL (app running?)"

# --- 2) Parse: active profile ---
if command -v jq &>/dev/null; then
  PROFILE=$(echo "$HEALTH_JSON" | jq -r '.profile[0] // empty')
  BOOTSTRAP=$(echo "$HEALTH_JSON" | jq -r '.kafkaBootstrap // empty')
  STREAM_THREADS=$(echo "$HEALTH_JSON" | jq -r '.streamThreads // empty')
  STATE_DIR=$(echo "$CONFIG_JSON" | jq -r '.["spring.kafka.streams.state-dir"] // empty')
else
  PROFILE=$(echo "$HEALTH_JSON" | grep -o '"profile"[[:space:]]*:[[:space:]]*\[[^]]*]' | grep -o '"[^"]*"' | tail -1 | tr -d '"')
  BOOTSTRAP=$(echo "$HEALTH_JSON" | grep -o '"kafkaBootstrap"[[:space:]]*:[[:space:]]*"[^"]*"' | sed 's/.*:"\([^"]*\)".*/\1/')
  STREAM_THREADS=$(echo "$HEALTH_JSON" | grep -o '"streamThreads"[[:space:]]*:[[:space:]]*"[^"]*"' | sed 's/.*:"\([^"]*\)".*/\1/')
  STATE_DIR=$(echo "$CONFIG_JSON" | grep -o '"spring.kafka.streams.state-dir"[[:space:]]*:[[:space:]]*"[^"]*"' | sed 's/.*:"\([^"]*\)".*/\1/')
fi

# --- 1) Assert active Spring profile (dev or prod) ---
[[ -n "$PROFILE" ]] || err "Active profile missing (expected dev or prod)"
[[ "$PROFILE" == "dev" || "$PROFILE" == "prod" ]] || err "Active profile must be dev or prod, got: $PROFILE"
echo "OK profile=$PROFILE"

# --- 2) Assert bootstrap servers do NOT contain localhost when profile=dev/prod ---
[[ -n "$BOOTSTRAP" ]] || err "Bootstrap servers missing"
if echo "$BOOTSTRAP" | grep -qi localhost; then
  err "Bootstrap servers must not contain localhost when profile is dev/prod; got: $BOOTSTRAP"
fi
echo "OK bootstrap=$BOOTSTRAP"

# --- 3) Assert state-dir exists and is writable ---
[[ -n "$STATE_DIR" ]] || err "state-dir missing in config"
CHECK_DIR="${STATE_DIR_OVERRIDE:-$STATE_DIR}"
if [[ "$CHECK_DIR" != /* ]]; then
  CHECK_DIR="$(pwd)/${CHECK_DIR}"
fi
[[ -d "$CHECK_DIR" ]] || err "state-dir does not exist or is not a directory: $CHECK_DIR"
[[ -w "$CHECK_DIR" ]] || err "state-dir is not writable: $CHECK_DIR"
echo "OK state-dir=$CHECK_DIR (exists, writable)"

# --- 4) Assert num.stream.threads matches profile expectation ---
[[ -n "$STREAM_THREADS" ]] || err "num.stream.threads missing"
EXPECTED_THREADS=1
[[ "$PROFILE" == "prod" ]] && EXPECTED_THREADS=12
[[ "$STREAM_THREADS" == "$EXPECTED_THREADS" ]] || err "num.stream.threads must be $EXPECTED_THREADS for profile=$PROFILE; got: $STREAM_THREADS"
echo "OK num.stream.threads=$STREAM_THREADS (expected $EXPECTED_THREADS for $PROFILE)"

echo ""
echo "All checks passed."
