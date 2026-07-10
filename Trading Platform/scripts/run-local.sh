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

export RISK_OBJECT_POOL_SIZE="${RISK_OBJECT_POOL_SIZE:-262144}"
export MDE_ORDER_PROJECTION_POOL_SIZE="${MDE_ORDER_PROJECTION_POOL_SIZE:-524288}"
export SKIP_STATE_HYDRATION="${SKIP_STATE_HYDRATION:-true}"

exec ./run_pro.sh
