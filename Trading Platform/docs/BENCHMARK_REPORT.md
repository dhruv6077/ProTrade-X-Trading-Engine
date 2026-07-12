# Vision Trader Benchmark Report

**Verification date:** 2026-07-11<br>
**Transport:** Netty WebSocket `/ws/orders`<br>
**Harness:** `tests/load/benchmark.js` through `scripts/verify-all.sh`

## Test Contract

The final verification runs model a bounded order-entry workload rather than an unbounded socket flood. Each virtual user keeps a correlated WebSocket session, submits terminal IOC orders at a controlled interval, and waits within a bounded in-flight window for an execution-report acknowledgement.

| Setting | Value |
| --- | ---: |
| Concurrent virtual users | 500 |
| Per-VU order interval | 200ms |
| Saturation hold | 30s |
| Verification passes | 3 |
| Accepted ACK p99 SLA | `<50ms` |
| ACK timeout threshold | `0%` |
| Connection failure threshold | `0%` |

## Triple-Verification Results

| Run | Accepted p95 | Accepted p99 | Accepted ACKs | Rejected ACKs | Load-shed rejects | ACK timeouts | Connection failures | Total GC pause |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 5ms | 18ms | 98,294 | 0 | 0 | 0% | 0% | 0.119ms |
| 2 | 5ms | 10ms | 98,500 | 0 | 0 | 0% | 0% | 0.166ms |
| 3 | 6ms | 31ms | 98,500 | 0 | 0 | 0% | 0% | 0.154ms |
| **Aggregate** | **5.33ms avg** | **19.67ms avg / 31ms worst** | **295,294** | **0** | **0** | **0%** | **0%** | **0.146ms avg** |

All three runs passed the accepted-order p99 SLA. Every submitted order received a correlated accepted, rejected, or overload response; this profile produced accepted acknowledgements only.

## Runtime Profile

The verification runner configures bounded pools before starting the JVM:

```bash
RISK_OBJECT_POOL_SIZE=262144
MDE_ORDER_PROJECTION_POOL_SIZE=524288
ORDER_BOOK_INITIAL_CAPACITY=262144
```

Local macOS benchmark runs use a phased backoff wait strategy to avoid burning all available cores while Netty, k6, and JFR share the machine. Production Linux defaults to a yielding strategy unless explicitly overridden.

## Reproduce

Run the Java correctness suite and all three benchmark passes:

```bash
./scripts/verify-all.sh
```

Run one profiled pass:

```bash
./scripts/run-profile.sh
```

The scripts write local k6/JFR artifacts under `diagnostics/`, which is intentionally excluded from Git because recordings are machine-specific and can be large.

## Interpretation

- The figures validate the repository's calibrated 500-VU WebSocket profile on the recorded development machine.
- They do not claim exchange colocated latency, maximum throughput, or production capacity across arbitrary hardware.
- HTTP/Javalin remains available for dashboard and compatibility workflows; the optimized order-entry benchmark targets the persistent Netty WebSocket path.
