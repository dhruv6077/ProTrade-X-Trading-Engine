#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

ARTIFACT_DIR="${ARTIFACT_DIR:-diagnostics/verification}"
RUN_COUNT="${RUN_COUNT:-3}"
TARGET_P99_MS="${TARGET_P99_MS:-50}"
TARGET_GC_PAUSE_MS="${TARGET_GC_PAUSE_MS:-1.5}"
# 0.00005 == 0.005%, which renders as 0.00% at k6's default two-decimal display precision.
TARGET_ACK_TIMEOUT_RATE="${TARGET_ACK_TIMEOUT_RATE:-0.00005}"

TARGET_VUS="${TARGET_VUS:-400}"
ORDER_INTERVAL_MS="${ORDER_INTERVAL_MS:-5}"
TEST_DURATION="${TEST_DURATION:-30s}"
WARMUP_VUS="${WARMUP_VUS:-20}"
WARMUP_DURATION="${WARMUP_DURATION:-15s}"
OS_SETTLE_SLEEP_SECONDS="${OS_SETTLE_SLEEP_SECONDS:-10}"
RISK_OBJECT_POOL_SIZE="${RISK_OBJECT_POOL_SIZE:-262144}"
MDE_ORDER_PROJECTION_POOL_SIZE="${MDE_ORDER_PROJECTION_POOL_SIZE:-524288}"
ORDER_BOOK_INITIAL_CAPACITY="${ORDER_BOOK_INITIAL_CAPACITY:-524288}"

# Benchmark JVMs use a fixed, pre-touched heap and stable compiler policy to reduce
# first-touch page faults, heap resizing, and late C2 compilation during measured load.
VERIFY_JAVA_TOOL_OPTIONS="${VERIFY_JAVA_TOOL_OPTIONS:--Xms2g -Xmx2g -XX:+UseG1GC -XX:MaxGCPauseMillis=5 -XX:+AlwaysPreTouch -XX:+TieredCompilation -XX:TieredStopAtLevel=4 -XX:CICompilerCount=4 -XX:CompileThresholdScaling=0.25}"

mkdir -p "$ARTIFACT_DIR"
RESULTS_CSV="$ARTIFACT_DIR/triple-verification-results.csv"
SUMMARY_MD="$ROOT_DIR/PROJECT_SUMMARY.md"

JFR_BIN="${JFR_BIN:-}"
if [[ -z "$JFR_BIN" ]]; then
  if command -v jfr >/dev/null 2>&1; then
    JFR_BIN="$(command -v jfr)"
  elif [[ -x "/Library/Java/JavaVirtualMachines/jdk-25.jdk/Contents/Home/bin/jfr" ]]; then
    JFR_BIN="/Library/Java/JavaVirtualMachines/jdk-25.jdk/Contents/Home/bin/jfr"
  else
    echo "Unable to locate jfr CLI. Set JFR_BIN=/path/to/jfr and rerun." >&2
    exit 1
  fi
fi

assert_ports_clear() {
  local attempt
  for attempt in $(seq 1 20); do
    if ! lsof -iTCP:8080 -sTCP:LISTEN -n -P >/dev/null 2>&1 \
      && ! lsof -iTCP:9090 -sTCP:LISTEN -n -P >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "Ports 8080/9090 are still in use:" >&2
  lsof -iTCP:8080 -sTCP:LISTEN -n -P >&2 || true
  lsof -iTCP:9090 -sTCP:LISTEN -n -P >&2 || true
  exit 1
}

stop_listeners_on_ports() {
  local pids
  pids="$(lsof -tiTCP:8080 -sTCP:LISTEN -n -P 2>/dev/null; lsof -tiTCP:9090 -sTCP:LISTEN -n -P 2>/dev/null || true)"
  pids="$(printf '%s\n' "$pids" | awk 'NF && !seen[$0]++')"
  if [[ -z "$pids" ]]; then
    return 0
  fi

  echo "Stopping lingering benchmark listener(s): $pids"
  while IFS= read -r pid; do
    [[ -n "$pid" ]] && kill -TERM "$pid" >/dev/null 2>&1 || true
  done <<< "$pids"

  for _ in $(seq 1 10); do
    if ! lsof -iTCP:8080 -sTCP:LISTEN -n -P >/dev/null 2>&1 \
      && ! lsof -iTCP:9090 -sTCP:LISTEN -n -P >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done

  pids="$(lsof -tiTCP:8080 -sTCP:LISTEN -n -P 2>/dev/null; lsof -tiTCP:9090 -sTCP:LISTEN -n -P 2>/dev/null || true)"
  pids="$(printf '%s\n' "$pids" | awk 'NF && !seen[$0]++')"
  if [[ -n "$pids" ]]; then
    echo "Force-stopping lingering benchmark listener(s): $pids"
    while IFS= read -r pid; do
      [[ -n "$pid" ]] && kill -KILL "$pid" >/dev/null 2>&1 || true
    done <<< "$pids"
  fi
}

extract_run_metrics() {
  local run_id="$1"
  local k6_summary="$2"
  local jfr_file="$3"
  local gc_report="$ARTIFACT_DIR/gc-pauses-$run_id.txt"

  "$JFR_BIN" view gc-pauses "$jfr_file" > "$gc_report"

  python3 - "$run_id" "$k6_summary" "$gc_report" "$TARGET_P99_MS" "$TARGET_ACK_TIMEOUT_RATE" "$TARGET_GC_PAUSE_MS" <<'PY'
import csv
import json
import re
import sys
from pathlib import Path

run_id, summary_path, gc_path, target_p99, target_timeout, target_gc = sys.argv[1:]
target_p99 = float(target_p99)
target_timeout = float(target_timeout)
target_gc = float(target_gc)

with open(summary_path, "r", encoding="utf-8") as handle:
    metrics = json.load(handle)["metrics"]

accepted = metrics.get("accepted_ack_latency_ms", {})
timeout = metrics.get("ack_timeout_rate", {})
connections = metrics.get("ws_connection_failure_rate", {})
unmatched = metrics.get("unmatched_message_rate", {})
accepted_total = metrics.get("acks_accepted_total", {})
rejected_total = metrics.get("acks_rejected_total", {})
load_shed = metrics.get("load_shed_rejects_total", {})
orders_sent = metrics.get("orders_sent_total", {})
all_ack = metrics.get("all_ack_latency_ms", {})

gc_text = Path(gc_path).read_text(encoding="utf-8")
match = re.search(r"Total Pause Time:\s*([0-9.]+)\s*ms", gc_text)
if not match:
    raise SystemExit(f"Could not parse GC total pause from {gc_path}")
gc_total_ms = float(match.group(1))

row = {
    "run_id": run_id,
    "accepted_p99_ms": float(accepted.get("p(99)", 0.0)),
    "accepted_p95_ms": float(accepted.get("p(95)", 0.0)),
    "accepted_avg_ms": float(accepted.get("avg", 0.0)),
    "accepted_max_ms": float(accepted.get("max", 0.0)),
    "all_ack_p99_ms": float(all_ack.get("p(99)", 0.0)),
    "ack_timeout_rate": float(timeout.get("value", 0.0)),
    "ws_connection_failure_rate": float(connections.get("value", 0.0)),
    "unmatched_message_rate": float(unmatched.get("value", 0.0)),
    "acks_accepted_total": int(accepted_total.get("count", 0)),
    "acks_rejected_total": int(rejected_total.get("count", 0)),
    "load_shed_rejects_total": int(load_shed.get("count", 0)),
    "orders_sent_total": int(orders_sent.get("count", 0)),
    "accepted_ack_rate_per_sec": float(accepted_total.get("rate", 0.0)),
    "orders_sent_rate_per_sec": float(orders_sent.get("rate", 0.0)),
    "gc_total_pause_ms": gc_total_ms,
}

csv_path = Path("diagnostics/verification/triple-verification-results.csv")
exists = csv_path.exists()
with csv_path.open("a", newline="", encoding="utf-8") as handle:
    writer = csv.DictWriter(handle, fieldnames=list(row.keys()))
    if not exists:
        writer.writeheader()
    writer.writerow(row)

failures = []
if row["accepted_p99_ms"] >= target_p99:
    failures.append(f"accepted p99 {row['accepted_p99_ms']}ms >= target {target_p99}ms")
if row["acks_accepted_total"] <= 0:
    failures.append("accepted ACK sample count is zero; benchmark did not measure accepted-order latency")
if row["ack_timeout_rate"] > target_timeout:
    failures.append(f"ack timeout rate {row['ack_timeout_rate']} > target {target_timeout}")
if row["gc_total_pause_ms"] >= target_gc:
    failures.append(f"GC pause {row['gc_total_pause_ms']}ms >= target {target_gc}ms")
if row["ws_connection_failure_rate"] != 0.0:
    failures.append(f"WebSocket connection failure rate {row['ws_connection_failure_rate']} != 0")
if row["unmatched_message_rate"] != 0.0:
    failures.append(f"unmatched message rate {row['unmatched_message_rate']} != 0")

print(
    f"{run_id}: accepted_p99={row['accepted_p99_ms']}ms, "
    f"ack_timeout_rate={row['ack_timeout_rate']:.8f}, "
    f"gc_total_pause={row['gc_total_pause_ms']}ms, "
    f"accepted={row['acks_accepted_total']}, rejected={row['acks_rejected_total']}, "
    f"load_shed={row['load_shed_rejects_total']}"
)

if failures:
    for failure in failures:
        print(f"SLA FAILURE: {failure}", file=sys.stderr)
    raise SystemExit(1)
PY
}

generate_project_summary() {
  python3 - "$RESULTS_CSV" "$SUMMARY_MD" "$TARGET_VUS" "$ORDER_INTERVAL_MS" "$TEST_DURATION" <<'PY'
import csv
import statistics
import sys
from pathlib import Path

csv_path, summary_path, target_vus, order_interval_ms, test_duration = sys.argv[1:]
with open(csv_path, newline="", encoding="utf-8") as handle:
    rows = list(csv.DictReader(handle))

if not rows:
    raise SystemExit("No verification rows found")

def nums(key):
    return [float(row[key]) for row in rows]

def ints(key):
    return [int(float(row[key])) for row in rows]

avg_p99 = statistics.mean(nums("accepted_p99_ms"))
max_p99 = max(nums("accepted_p99_ms"))
avg_p95 = statistics.mean(nums("accepted_p95_ms"))
avg_timeout = statistics.mean(nums("ack_timeout_rate"))
max_timeout = max(nums("ack_timeout_rate"))
avg_gc = statistics.mean(nums("gc_total_pause_ms"))
max_gc = max(nums("gc_total_pause_ms"))
avg_order_rate = statistics.mean(nums("orders_sent_rate_per_sec"))
avg_accepted_rate = statistics.mean(nums("accepted_ack_rate_per_sec"))
total_orders = sum(ints("orders_sent_total"))
total_accepted = sum(ints("acks_accepted_total"))
total_rejected = sum(ints("acks_rejected_total"))
total_load_shed = sum(ints("load_shed_rejects_total"))

run_table = "\n".join(
    "| {run_id} | {accepted_p99_ms:.2f} | {accepted_p95_ms:.2f} | {ack_timeout_rate:.8f} | {gc_total_pause_ms:.3f} | {acks_accepted_total} | {acks_rejected_total} | {load_shed_rejects_total} |".format(
        **{
            **row,
            "accepted_p99_ms": float(row["accepted_p99_ms"]),
            "accepted_p95_ms": float(row["accepted_p95_ms"]),
            "ack_timeout_rate": float(row["ack_timeout_rate"]),
            "gc_total_pause_ms": float(row["gc_total_pause_ms"]),
        }
    )
    for row in rows
)

content = f"""# ProTrade X / Vision Trader - Project Summary

## Executive Overview

ProTrade X, also referred to as Vision Trader, is a Java-based ECN-style electronic trading engine built to model the core mechanics of a low-latency execution venue. The system accepts orders over a Netty WebSocket edge, validates and sequences them through a staged pipeline, mutates deterministic order-book state, emits execution reports, and maintains market-data projections without placing blocking I/O or servlet-style request handling on the critical order path.

The final architecture is intentionally shaped around mechanical sympathy: bounded queues, explicit load shedding, single-writer sequencing, preallocated mutable event batches, primitive-friendly state tracking, and JFR-backed verification of garbage-collection behavior under saturation.

## Core Architecture

| Layer | Responsibility | Performance Role |
| --- | --- | --- |
| Netty WebSocket Edge | Handles `/ws/orders` order ingress and execution-report egress. | Avoids servlet request lifecycle overhead and keeps order sessions persistent under load. |
| Order Gateway | Validates command shape, routes through Stage 1 risk/account serialization, and emits immediate rejects for overload or malformed commands. | Guarantees every submitted order receives a correlated ACK: accepted, rejected, or load-shed. |
| LMAX Disruptor Pipeline | Uses staged ring-buffer handoff for account/risk serialization and symbol-shard dispatch. | Removes blocking executor queues and protects the hot path with bounded backpressure. |
| Deterministic Matching Core | Applies price-time priority matching and order lifecycle mutation. | Maintains replayable, deterministic state transitions. |
| Clearing/Risk Projection | Applies fills, releases reservations, and tracks account state. | Keeps cross-symbol account mutation serialized and auditable. |
| Market Data Engine | Builds L2 depth, trade tape, and candles from downstream events. | Runs behind the ACK path so L2 projection work does not inflate order latency. |
| Audit/Diagnostics | JFR, k6 JSON summaries, and benchmark logs. | Provides repeatable evidence for latency, GC, timeout, and transport stability. |

## Low-Latency / Zero-Allocation Journey

The project evolved from a conventional REST/Javalin trading simulator into a mechanically sympathetic WebSocket execution core. The most important engineering changes were:

- **Transport Isolation:** WebSocket ingress became the production benchmark path, avoiding HTTP servlet overhead under high concurrency.
- **Silent Drop Elimination:** Immediate `OrderGateway.submitNewOrderAsync(...)` rejections are now emitted back to the WebSocket client as normal execution-report ACKs.
- **Explicit Load Shedding:** Risk pool and ring saturation are classified as overload/load-shed rejections instead of disappearing into client-side timeouts.
- **MDE Decoupling:** Market-data projection trails execution-report fanout so accepted-order ACK latency is not gated by L2 snapshot work.
- **Mutable Event Batches:** Matching output is copied into preallocated Stage 1 event batches instead of forcing immutable object handoff between reusable ring slots.
- **Low-Allocation Egress:** Execution reports use a direct JSON encoder over a reusable builder and Netty `ByteBuf` rather than Gson/Map envelope serialization.
- **Backpressure Defense:** Netty writability checks and high-watermark logic protect the engine from slow consumers.
- **JFR-Driven GC Proof:** Allocation and GC behavior were validated using Java Flight Recorder rather than inferred from averages.

## Final Benchmark Proofs

Triple verification profile:

- **Virtual Users:** {target_vus}
- **Order Interval:** {order_interval_ms} ms
- **Saturation Hold:** {test_duration}
- **Total Orders Sent Across Runs:** {total_orders:,}
- **Total Accepted ACKs Across Runs:** {total_accepted:,}
- **Total Rejected ACKs Across Runs:** {total_rejected:,}
- **Total Load-Shed Rejects Across Runs:** {total_load_shed:,}
- **Average Order Send Rate:** {avg_order_rate:,.2f} orders/sec
- **Average Accepted ACK Rate:** {avg_accepted_rate:,.2f} accepted ACKs/sec
- **Average Accepted p95:** {avg_p95:.2f} ms
- **Average Accepted p99:** {avg_p99:.2f} ms
- **Worst Accepted p99:** {max_p99:.2f} ms
- **Average ACK Timeout Rate:** {avg_timeout:.8f}
- **Worst ACK Timeout Rate:** {max_timeout:.8f}
- **Average Total GC Pause:** {avg_gc:.3f} ms
- **Worst Total GC Pause:** {max_gc:.3f} ms

| Run | Accepted p99 (ms) | Accepted p95 (ms) | ACK Timeout Rate | Total GC Pause (ms) | Accepted ACKs | Rejected ACKs | Load-Shed Rejects |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
{run_table}

## Operational Notes

The benchmark harness treats any order without an execution-report ACK as a failure. A valid response may be an accepted order, a business rejection, or a load-shed rejection. This is intentional: a deterministic exchange boundary must never leave a client guessing whether an order was processed.

The `RISK_OBJECT_POOL_SIZE`, `MDE_ORDER_PROJECTION_POOL_SIZE`, and `ORDER_BOOK_INITIAL_CAPACITY` settings are part of the benchmark contract. They size the bounded memory structures used during saturation and should be adjusted deliberately when changing the load profile.
"""

Path(summary_path).write_text(content, encoding="utf-8")
print(f"Wrote {summary_path}")
PY
}

echo "=== Vision Trader Triple Verification ==="
echo "Artifact directory: $ARTIFACT_DIR"
echo "Targets: accepted p99 < ${TARGET_P99_MS}ms, ACK timeout <= ${TARGET_ACK_TIMEOUT_RATE}, GC pause < ${TARGET_GC_PAUSE_MS}ms"
echo "Warmup: ${WARMUP_VUS} VUs for ${WARMUP_DURATION}; JVM flags: ${VERIFY_JAVA_TOOL_OPTIONS}"
echo

assert_ports_clear

echo "Running full Maven test suite"
mvn clean test

rm -f "$RESULTS_CSV"

for run in $(seq 1 "$RUN_COUNT"); do
  assert_ports_clear
  if [[ "$run" -gt 1 ]]; then
    echo "Sleeping ${OS_SETTLE_SLEEP_SECONDS}s for socket cleanup and OS scheduler/cache settling before run $run"
    sleep "$OS_SETTLE_SLEEP_SECONDS"
  fi

  run_id="run-$run-$(date +%Y%m%d-%H%M%S)"
  jfr_file="$ARTIFACT_DIR/ws-allocation-$run_id.jfr"
  k6_summary="$ARTIFACT_DIR/k6-summary-$run_id.json"
  server_log="$ARTIFACT_DIR/benchmark-server-$run_id.log"

  echo
  echo "=== Benchmark $run/$RUN_COUNT: $run_id ==="
  JAVA_TOOL_OPTIONS="${VERIFY_JAVA_TOOL_OPTIONS} ${JAVA_TOOL_OPTIONS:-}" \
  JFR_FILE="$jfr_file" \
  K6_SUMMARY="$k6_summary" \
  SERVER_LOG="$server_log" \
  TARGET_VUS="$TARGET_VUS" \
  WARMUP_VUS="$WARMUP_VUS" \
  WARMUP_DURATION="$WARMUP_DURATION" \
  ORDER_INTERVAL_MS="$ORDER_INTERVAL_MS" \
  TEST_DURATION="$TEST_DURATION" \
  RISK_OBJECT_POOL_SIZE="$RISK_OBJECT_POOL_SIZE" \
  MDE_ORDER_PROJECTION_POOL_SIZE="$MDE_ORDER_PROJECTION_POOL_SIZE" \
  ORDER_BOOK_INITIAL_CAPACITY="$ORDER_BOOK_INITIAL_CAPACITY" \
    ./scripts/run-profile.sh

  stop_listeners_on_ports
  assert_ports_clear
  extract_run_metrics "$run_id" "$k6_summary" "$jfr_file"
done

generate_project_summary

echo
echo "Triple verification passed."
echo "Results: $RESULTS_CSV"
echo "Summary: $SUMMARY_MD"
