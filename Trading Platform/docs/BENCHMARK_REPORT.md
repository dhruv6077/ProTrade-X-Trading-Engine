# Vision Trader Benchmark Report

Date: 2026-07-02
Profile: 500 VU WebSocket order-entry stress test
Transport: Netty WebSocket `/ws/orders`
Script: `tests/load/ws-order-control-test.js`

## Final Validated Result

The WebSocket order path met the sub-50ms p99 SLA under 500 virtual users.

| Metric | Result |
| --- | ---: |
| Accepted order p99 | 28ms |
| Overall stress p99 | 28ms |
| Median ACK latency | 4ms |
| p95 ACK latency | 10ms |
| Max ACK latency | 120ms |
| WebSocket connection failures | 0% |
| ACK timeouts | 0% |
| Unmatched messages | 0% |
| Orders sent | 843,200 |
| Orders acknowledged | 843,200 |

## Required Benchmark Settings

The validated run used larger preallocated pools to avoid hot-path pool exhaustion during sustained 500 VU load:

```bash
RISK_OBJECT_POOL_SIZE=262144
MDE_ORDER_PROJECTION_POOL_SIZE=524288
```

These are set by default in:

```bash
scripts/run-benchmark-server.sh
```

## Reproduction Commands

Start the benchmark server:

```bash
./scripts/run-benchmark-server.sh
```

Run the WebSocket load test:

```bash
./scripts/run-ws-load-test.sh
```

Optional JFR allocation recording:

```bash
jcmd <PID> JFR.start \
  name=ws_allocation \
  settings=diagnostics/jfr/ws-tlab-allocation.jfc \
  filename=diagnostics/jfr/ws-allocation-after-risk-mde-pools.jfr \
  disk=true \
  path-to-gc-roots=false
```

Stop the recording:

```bash
jcmd <PID> JFR.stop name=ws_allocation
```

## JFR Confirmation

Final recording:

```text
diagnostics/jfr/ws-allocation-after-risk-mde-pools-20260702-221315.jfr
```

Confirmed:

- `borrowCashReservation()` no longer appears as an allocation hotspot.
- `OrderProjection pool exhausted` no longer appears.
- No exception/logging storm occurred during the passing run.
- Transport stability remained clean: no connection failures, ACK timeouts, or unmatched messages.

## Notes

This benchmark validates the optimized WebSocket path, not the HTTP/Javalin path. The run confirms that the Netty WebSocket ingress plus Disruptor-backed core can sustain the 500 VU profile while staying comfortably below the 50ms p99 SLA.
