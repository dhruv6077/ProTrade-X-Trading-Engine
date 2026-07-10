#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

if [[ -f ".env.local" ]]; then
  set -a
  # shellcheck disable=SC1091
  source ".env.local"
  set +a
fi

BASE_URL="${BASE_URL:-http://localhost:8080}"
EXPECTED_STATUS="${EXPECTED_STATUS:-202}"
TARGET_VUS="${TARGET_VUS:-500}"
TEST_DURATION="${TEST_DURATION:-60s}"
K6_TRADER_CASH="${K6_TRADER_CASH:-100000000000000}"

k6 run \
  -e BASE_URL="$BASE_URL" \
  -e EXPECTED_STATUS="$EXPECTED_STATUS" \
  -e TARGET_VUS="$TARGET_VUS" \
  -e TEST_DURATION="$TEST_DURATION" \
  -e K6_TRADER_CASH="$K6_TRADER_CASH" \
  tests/load/stress-test.js
