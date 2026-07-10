#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

ARTIFACT_DIR="${ARTIFACT_DIR:-diagnostics/jfr}"
TIMESTAMP="$(date +%Y%m%d-%H%M%S)"
JFR_FILE="${JFR_FILE:-$ARTIFACT_DIR/ws-allocation-$TIMESTAMP.jfr}"
SERVER_LOG="${SERVER_LOG:-$ARTIFACT_DIR/benchmark-server-$TIMESTAMP.log}"
K6_SUMMARY="${K6_SUMMARY:-$ARTIFACT_DIR/k6-summary-$TIMESTAMP.json}"
JFC_TEMPLATE="${JFC_TEMPLATE:-diagnostics/jfr/ws-tlab-allocation.jfc}"

BASE_URL="${BASE_URL:-http://localhost:8080}"
WS_URL="${WS_URL:-ws://localhost:9090/ws/orders}"
TARGET_VUS="${TARGET_VUS:-400}"
WARMUP_VUS="${WARMUP_VUS:-50}"
WARMUP_DURATION="${WARMUP_DURATION:-30s}"
RAMP_STEP_DURATION="${RAMP_STEP_DURATION:-10s}"
TEST_DURATION="${TEST_DURATION:-30s}"
ORDER_INTERVAL_MS="${ORDER_INTERVAL_MS:-5}"
ACK_TIMEOUT_MS="${ACK_TIMEOUT_MS:-5000}"
CLIENT_POOL_SIZE="${CLIENT_POOL_SIZE:-$TARGET_VUS}"
K6_TRADER_CASH="${K6_TRADER_CASH:-1000000000000000}"
K6_TRADER_POSITION="${K6_TRADER_POSITION:-1000000000}"
K6_TRADER_SHORT_SELLING="${K6_TRADER_SHORT_SELLING:-true}"
RISK_OBJECT_POOL_SIZE="${RISK_OBJECT_POOL_SIZE:-262144}"
MDE_ORDER_PROJECTION_POOL_SIZE="${MDE_ORDER_PROJECTION_POOL_SIZE:-524288}"
ORDER_BOOK_INITIAL_CAPACITY="${ORDER_BOOK_INITIAL_CAPACITY:-524288}"

mkdir -p "$ARTIFACT_DIR"

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

JFR_SETTINGS="settings=$JFC_TEMPLATE"
if [[ ! -f "$JFC_TEMPLATE" ]]; then
  echo "JFR template not found at $JFC_TEMPLATE; falling back to profile settings."
  JFR_SETTINGS="settings=profile"
fi

export JAVA_TOOL_OPTIONS="${JAVA_TOOL_OPTIONS:-} -Dlogback.configurationFile=src/logback-loadtest.xml -XX:StartFlightRecording=name=ws-allocation,$JFR_SETTINGS,filename=$JFR_FILE,dumponexit=true,disk=true,maxage=2m,maxsize=512m,path-to-gc-roots=false"

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

find_jfr_pid() {
  if ! command -v jcmd >/dev/null 2>&1; then
    return 1
  fi
  jcmd -l | awk '/exec.mainClass=Main|org.codehaus.plexus.classworlds.launcher.Launcher|Main/ { pid=$1 } END { if (pid != "") print pid }'
}

dump_jfr() {
  local pid
  pid="$(find_jfr_pid || true)"
  if [[ -z "${pid:-}" ]]; then
    echo "Unable to locate Java process for explicit JFR dump; relying on dumponexit/disk=true."
    return 0
  fi
  echo "Dumping JFR from pid=$pid"
  jcmd "$pid" JFR.dump name=ws-allocation filename="$JFR_FILE" >/dev/null 2>&1 || true
  jcmd "$pid" JFR.stop name=ws-allocation >/dev/null 2>&1 || true
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
  -e CLIENT_POOL_SIZE="$CLIENT_POOL_SIZE" \
  -e K6_TRADER_CASH="$K6_TRADER_CASH" \
  -e K6_TRADER_POSITION="$K6_TRADER_POSITION" \
  -e K6_TRADER_SHORT_SELLING="$K6_TRADER_SHORT_SELLING" \
  tests/load/benchmark.js || K6_STATUS=$?

echo "Flushing JFR before server shutdown"
dump_jfr

echo "Stopping server after JFR flush"
cleanup
trap - EXIT

echo "Artifacts:"
echo "  JFR: $JFR_FILE"
echo "  k6 summary: $K6_SUMMARY"
echo "  server log: $SERVER_LOG"

if command -v jfr >/dev/null 2>&1 && [[ -f "$JFR_FILE" ]]; then
  echo
  echo "JFR GC pauses:"
  jfr view gc-pauses "$JFR_FILE" || true
  echo
  echo "JFR allocation by class:"
  jfr view allocation-by-class "$JFR_FILE" | sed -n '1,80p' || true
  echo
  echo "JFR hot methods:"
  jfr view hot-methods "$JFR_FILE" | sed -n '1,80p' || true
else
  echo "jfr CLI unavailable or recording missing; open $JFR_FILE in JDK Mission Control."
fi

exit "$K6_STATUS"
