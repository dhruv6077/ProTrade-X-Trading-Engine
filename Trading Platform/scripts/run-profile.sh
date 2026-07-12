#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

ARTIFACT_DIR="${ARTIFACT_DIR:-diagnostics/jfr}"
TIMESTAMP="$(date +%Y%m%d-%H%M%S)"
JFR_FILE="${JFR_FILE:-$ARTIFACT_DIR/ws-allocation-$TIMESTAMP.jfr}"
JFR_LIVE_FILE="${JFR_LIVE_FILE:-$JFR_FILE.live}"
SERVER_LOG="${SERVER_LOG:-$ARTIFACT_DIR/benchmark-server-$TIMESTAMP.log}"
K6_SUMMARY="${K6_SUMMARY:-$ARTIFACT_DIR/k6-summary-$TIMESTAMP.json}"
JFC_TEMPLATE="${JFC_TEMPLATE:-}"

BASE_URL="${BASE_URL:-http://localhost:8080}"
WS_URL="${WS_URL:-ws://localhost:9090/ws/orders}"
TARGET_VUS="${TARGET_VUS:-500}"
WARMUP_VUS="${WARMUP_VUS:-50}"
WARMUP_DURATION="${WARMUP_DURATION:-30s}"
RAMP_STEP_DURATION="${RAMP_STEP_DURATION:-10s}"
TEST_DURATION="${TEST_DURATION:-30s}"
ORDER_INTERVAL_MS="${ORDER_INTERVAL_MS:-200}"
ACK_TIMEOUT_MS="${ACK_TIMEOUT_MS:-5000}"
SMOKE_ORDER_TYPE="${SMOKE_ORDER_TYPE:-IOC}"
CLIENT_POOL_SIZE="${CLIENT_POOL_SIZE:-$TARGET_VUS}"
K6_TRADER_CASH="${K6_TRADER_CASH:-1000000000000000}"
K6_TRADER_POSITION="${K6_TRADER_POSITION:-1000000000}"
K6_TRADER_SHORT_SELLING="${K6_TRADER_SHORT_SELLING:-true}"
RISK_OBJECT_POOL_SIZE="${RISK_OBJECT_POOL_SIZE:-262144}"
MDE_ORDER_PROJECTION_POOL_SIZE="${MDE_ORDER_PROJECTION_POOL_SIZE:-524288}"
ORDER_BOOK_INITIAL_CAPACITY="${ORDER_BOOK_INITIAL_CAPACITY:-524288}"

mkdir -p "$ARTIFACT_DIR"
rm -f "$JFR_FILE" "$JFR_LIVE_FILE"

# Keep unattended macOS benchmark runs from being suspended mid-scenario.
# The assertion automatically ends when this script exits.
if command -v caffeinate >/dev/null 2>&1; then
  caffeinate -dimsu -w "$$" >/dev/null 2>&1 &
fi

if lsof -iTCP:8080 -sTCP:LISTEN -n -P >/dev/null 2>&1 || lsof -iTCP:9090 -sTCP:LISTEN -n -P >/dev/null 2>&1; then
  echo "Ports 8080/9090 are already in use. Stop the existing server before profiling." >&2
  lsof -iTCP:8080 -sTCP:LISTEN -n -P || true
  lsof -iTCP:9090 -sTCP:LISTEN -n -P || true
  exit 1
fi

export ENABLE_LOAD_TEST_ADMIN="${ENABLE_LOAD_TEST_ADMIN:-true}"
export SKIP_STATE_HYDRATION="${SKIP_STATE_HYDRATION:-true}"
export CLEAN_ROOM_ORDERS="${CLEAN_ROOM_ORDERS:-false}"
export RISK_OBJECT_POOL_SIZE
export MDE_ORDER_PROJECTION_POOL_SIZE
export ORDER_BOOK_INITIAL_CAPACITY

JFR_SETTINGS="settings=profile"
if [[ -n "$JFC_TEMPLATE" ]]; then
  if [[ ! -f "$JFC_TEMPLATE" ]]; then
    echo "JFR template not found at $JFC_TEMPLATE" >&2
    exit 1
  fi
  JFR_SETTINGS="settings=$JFC_TEMPLATE"
fi

export JAVA_TOOL_OPTIONS="${JAVA_TOOL_OPTIONS:-} -Dlogback.configurationFile=src/logback-loadtest.xml -XX:StartFlightRecording=name=ws-allocation,$JFR_SETTINGS,filename=$JFR_LIVE_FILE,dumponexit=true,disk=true,maxage=2m,maxsize=512m,path-to-gc-roots=false"

echo "Starting profiled benchmark server"
echo "  JFR_FILE=$JFR_FILE"
echo "  SERVER_LOG=$SERVER_LOG"
echo "  TARGET_VUS=$TARGET_VUS"
echo "  ORDER_INTERVAL_MS=$ORDER_INTERVAL_MS"
echo "  RISK_OBJECT_POOL_SIZE=$RISK_OBJECT_POOL_SIZE"
echo "  MDE_ORDER_PROJECTION_POOL_SIZE=$MDE_ORDER_PROJECTION_POOL_SIZE"
echo "  ORDER_BOOK_INITIAL_CAPACITY=$ORDER_BOOK_INITIAL_CAPACITY"

./scripts/run-benchmark-server.sh >"$SERVER_LOG" 2>&1 &
SERVER_PID=$!
SERVER_STOPPED=false

stop_jfr() {
  local jcmd_bin
  jcmd_bin="${JCMD_BIN:-}"
  if [[ -z "$jcmd_bin" ]]; then
    if command -v jcmd >/dev/null 2>&1; then
      jcmd_bin="$(command -v jcmd)"
    elif [[ -x "/Library/Java/JavaVirtualMachines/jdk-25.jdk/Contents/Home/bin/jcmd" ]]; then
      jcmd_bin="/Library/Java/JavaVirtualMachines/jdk-25.jdk/Contents/Home/bin/jcmd"
    elif [[ -x "/opt/homebrew/opt/openjdk/bin/jcmd" ]]; then
      jcmd_bin="/opt/homebrew/opt/openjdk/bin/jcmd"
    else
      echo "jcmd unavailable; relying on dumponexit recording finalization."
      return 0
    fi
  fi
  if ! kill -0 "$SERVER_PID" >/dev/null 2>&1; then
    echo "Benchmark JVM already stopped; relying on dumponexit recording finalization."
    return 0
  fi
  echo "Dumping and stopping JFR recording in pid=$SERVER_PID"
  "$jcmd_bin" "$SERVER_PID" JFR.dump name=ws-allocation filename="$JFR_FILE" >/dev/null
  "$jcmd_bin" "$SERVER_PID" JFR.stop name=ws-allocation >/dev/null
  if [[ -s "$JFR_FILE" ]]; then
    rm -f "$JFR_LIVE_FILE"
  fi
}

cleanup() {
  if [[ "$SERVER_STOPPED" == "true" ]]; then
    return
  fi
  SERVER_STOPPED=true
  if kill -0 "$SERVER_PID" >/dev/null 2>&1; then
    echo "Stopping benchmark server pid=$SERVER_PID"
    kill -TERM "$SERVER_PID" >/dev/null 2>&1 || true
    for _ in $(seq 1 10); do
      if ! kill -0 "$SERVER_PID" >/dev/null 2>&1; then
        wait "$SERVER_PID" >/dev/null 2>&1 || true
        return
      fi
      sleep 1
    done
    echo "Server did not stop after 10s; forcing shutdown"
    kill -KILL "$SERVER_PID" >/dev/null 2>&1 || true
    wait "$SERVER_PID" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

echo "Waiting for backend ports"
for _ in $(seq 1 90); do
  if lsof -iTCP:8080 -sTCP:LISTEN -n -P >/dev/null 2>&1 && \
     lsof -iTCP:9090 -sTCP:LISTEN -n -P >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

if ! lsof -iTCP:8080 -sTCP:LISTEN -n -P >/dev/null 2>&1 || \
   ! lsof -iTCP:9090 -sTCP:LISTEN -n -P >/dev/null 2>&1; then
  echo "Server did not open ports 8080/9090. Last server log lines:" >&2
  tail -80 "$SERVER_LOG" >&2 || true
  exit 1
fi

echo "Running k6 saturation benchmark"
K6_STATUS=0
k6 run \
  --summary-export "$K6_SUMMARY" \
  -e BASE_URL="$BASE_URL" \
  -e WS_URL="$WS_URL" \
  -e TARGET_VUS="$TARGET_VUS" \
  -e WARMUP_VUS="$WARMUP_VUS" \
  -e WARMUP_DURATION="$WARMUP_DURATION" \
  -e RAMP_STEP_DURATION="$RAMP_STEP_DURATION" \
  -e TEST_DURATION="$TEST_DURATION" \
  -e ORDER_INTERVAL_MS="$ORDER_INTERVAL_MS" \
  -e ACK_TIMEOUT_MS="$ACK_TIMEOUT_MS" \
  -e SMOKE_ORDER_TYPE="$SMOKE_ORDER_TYPE" \
  -e CLIENT_POOL_SIZE="$CLIENT_POOL_SIZE" \
  -e K6_TRADER_CASH="$K6_TRADER_CASH" \
  -e K6_TRADER_POSITION="$K6_TRADER_POSITION" \
  -e K6_TRADER_SHORT_SELLING="$K6_TRADER_SHORT_SELLING" \
  tests/load/benchmark.js || K6_STATUS=$?

echo "Flushing JFR before server shutdown"
stop_jfr

echo "Stopping server after JFR flush"
cleanup
trap - EXIT

if [[ -s "$JFR_FILE" ]]; then
  sleep 1
  rm -f "$JFR_LIVE_FILE"
fi

echo "Artifacts:"
echo "  JFR: $JFR_FILE"
echo "  k6 summary: $K6_SUMMARY"
echo "  server log: $SERVER_LOG"

JFR_BIN="${JFR_BIN:-}"
if [[ -z "$JFR_BIN" ]]; then
  if command -v jfr >/dev/null 2>&1; then
    JFR_BIN="$(command -v jfr)"
  elif [[ -x "/Library/Java/JavaVirtualMachines/jdk-25.jdk/Contents/Home/bin/jfr" ]]; then
    JFR_BIN="/Library/Java/JavaVirtualMachines/jdk-25.jdk/Contents/Home/bin/jfr"
  elif [[ -x "/opt/homebrew/opt/openjdk/bin/jfr" ]]; then
    JFR_BIN="/opt/homebrew/opt/openjdk/bin/jfr"
  fi
fi

if [[ -n "$JFR_BIN" && -s "$JFR_FILE" ]]; then
  echo
  echo "JFR GC pauses:"
  "$JFR_BIN" view gc-pauses "$JFR_FILE" || true
  echo
  echo "JFR allocation by class:"
  "$JFR_BIN" view allocation-by-class "$JFR_FILE" | sed -n '1,80p' || true
  echo
  echo "JFR hot methods:"
  "$JFR_BIN" view hot-methods "$JFR_FILE" | sed -n '1,80p' || true
else
  echo "jfr CLI unavailable or recording missing; open $JFR_FILE in JDK Mission Control."
fi

exit "$K6_STATUS"
