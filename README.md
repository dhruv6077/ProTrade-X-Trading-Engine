# ProTrade X Trading Engine

ProTrade X, also referred to as Vision Trader in the runtime modules, is an ultra-low-latency ECN-style electronic trading exchange core written in Java. The project evolved from a traditional order book simulator into a deterministic, event-driven matching venue with a mechanically sympathetic execution pipeline.

The current architecture focuses on strict sequencing, cache-friendly data layouts, bounded object reuse, pre-trade risk serialization, deterministic replay, and low-jitter WebSocket order ingress. The latest validated milestone sustained a 500 virtual-user WebSocket order-entry profile with **843,200 orders fully acknowledged**, **0% drops/timeouts**, and a **28ms p99 ACK latency**.

## Executive Summary

| Capability | Implementation |
| --- | --- |
| Matching model | Deterministic price-time priority matching |
| Ingress | HTTP/Javalin plus Netty WebSocket order entry |
| Risk model | Serialized account/risk stage for buying-power and position validation |
| Concurrency model | Single-writer principles, staged handoff, isolated symbol matching |
| Hot-path allocation strategy | Flyweight mutation, bounded pools, primitive state tracking |
| Market data | L2 depth, trade tape, OHLCV, execution reports, account projections |
| Persistence | Append-only command journaling with replay-oriented recovery |
| Observability | k6, JFR, JUnit, Playwright, JMH-ready benchmark scaffolding |

## Architecture

```mermaid
flowchart LR
    HTTP["HTTP API /api/orders"] --> GATEWAY["OrderGateway"]
    WS["Netty WebSocket /ws/orders"] --> GATEWAY
    GATEWAY --> RISK["Stage 1: Account / Risk / Sequencing"]
    RISK --> JOURNAL["Command Journal"]
    RISK --> MATCH["Stage 2: Symbol Matching"]
    MATCH --> DISPATCH["Event Dispatch / Egress"]
    DISPATCH --> CLEARING["Clearing Service"]
    DISPATCH --> MDE["Market Data Engine"]
    DISPATCH --> CLIENTS["Dashboard / WebSocket / SSE Clients"]
    JOURNAL --> REPLAY["Deterministic Replay"]
```

## System Dashboard & Telemetry

The runtime includes a Javalin-served operational dashboard for inspecting venue state while the Netty WebSocket order path and event-driven projections are under load. Drop screenshots into `Trading Platform/docs/assets/` using the filenames below to render this section on GitHub.

| Exchange Workstation | Observability Plane |
| --- | --- |
| <img src="Trading%20Platform/docs/assets/dashboard-l2-depth.png" alt="ProTrade X dashboard showing L2 depth, order entry, account state, and trade tape" width="100%"> | <img src="Trading%20Platform/docs/assets/grafana-latency-telemetry.png" alt="ProTrade X Grafana telemetry showing latency and transport health" width="100%"> |
| Real-time L2 order book, trade tape, order-entry controls, and account projections driven by the event-dispatch and market-data engine boundaries. | Prometheus/Grafana telemetry surface for validating WebSocket ACK latency, transport stability, throughput, and JVM behavior during benchmark profiles. |

| Matching & Pipeline Diagnostics |
| --- |
| <img src="Trading%20Platform/docs/assets/pipeline-diagnostics.png" alt="ProTrade X pipeline diagnostics showing shard health and latency metrics" width="100%"> |
| Runtime diagnostics tying symbol-shard health, queue depth, and latency reporting back to the single-writer execution model and replayable command stream. |

### Two-Stage Execution Pipeline

| Stage | Responsibility | Low-Latency Mechanics |
| --- | --- | --- |
| Ingress edge | Accept HTTP and WebSocket order flow | Netty WebSocket ingress is the optimized path; HTTP remains available for dashboard/API use and control-plane testing. |
| Stage 1: Account/Risk/Sequencing | Validate commands, serialize account mutation, reserve cash/positions, assign deterministic sequence numbers | Cross-symbol account state is protected by a serialized risk lane so concurrent symbol shards cannot overdraw the same client account. |
| Command journal | Persist the sequenced command stream for replay | Append-only command journaling is treated as the deterministic replay authority; relational persistence remains an async audit/read-model concern. |
| Stage 2: Matching | Route sequenced commands into isolated symbol matching logic | Symbol-local matching keeps price-time priority deterministic while avoiding global engine locks. |
| Event dispatch | Publish fills, cancels, rejects, account updates, and market data deltas | Mutable event batches and preallocated ring-buffer style handoff reduce defensive copy churn. |
| Market data engine | Maintain L2 depth, trade tape, OHLCV candles, and UI projections | Projection state is bounded and reused to avoid TLAB fragmentation under bursty market-data fanout. |
| Clearing service | Settle buyer/seller cash and positions from execution events | Post-trade settlement is event-driven and kept outside direct order book mutation. |

### Stage 1: Serialized Account and Risk Core

The first stage exists to solve a real exchange-engine problem: account state is global, while matching can be symbol-local. If two orders for the same client hit different symbols at the same time, buying power and reserved position state must still be updated deterministically.

Stage 1 handles:

- inbound command validation;
- account buying-power checks;
- sell-side available-position checks;
- cash and position reservation;
- monotonic sequencing;
- command journal append;
- routing to the matching stage only after risk acceptance.

This stage is intentionally serialized around account mutation. It favors correctness and deterministic replay over naive parallelism.

### Stage 2: Symbol Matching and Event Publication

The second stage executes the exchange venue logic:

- price-time priority matching;
- partial fills;
- IOC/FOK behavior;
- self-trade prevention;
- order cancel/modify/admin command handling;
- event publication into clearing, market data, and client execution-report paths.

Stage 2 is designed around the single-writer principle for each symbol book. Matching state is mutated by one logical owner, which avoids locks around the order book itself and keeps execution behavior replayable.

## Low-Latency & Zero-Allocation Design Principles

The 28ms p99 WebSocket order-path benchmark was achieved by progressively removing GC-heavy structures from the active order lifecycle.

| Principle | Applied Technique |
| --- | --- |
| Mechanical Sympathy | Prefer cache-friendly primitive fields and stable memory layouts over object graphs. |
| No Hot-Path `new` Calls | Mutate primitive quantities such as `leavesQty` and `cumQty` in place instead of allocating fresh `OrderState` or `OrderAccepted` objects per order. |
| Flyweight Pattern | Reuse mutable command/event carriers across staged processing boundaries rather than creating defensive immutable snapshots for every transition. |
| Lock-Free Object Pooling | Use bounded array-backed recycling for risk structures such as `CashReservation` and `RestingRiskOrder` to avoid repeated `HashMap$Node` and short-lived state allocations. |
| TLAB Fragmentation Mitigation | Remove allocation spikes discovered through JFR allocation profiling, especially in parsing, projection, risk reservation, and event formatting. |
| Egress Formatting Optimization | Replace expensive formatting paths such as `String.format()` with custom byte/cent appenders for outbound WebSocket payloads. |
| Backpressure Discipline | Prevent slow WebSocket consumers from stalling the core by monitoring Netty channel writability and protecting the event egress path. |
| Replay Determinism | Treat sequenced commands as the source of truth so state can be reconstructed in order after restart. |

## Verified Benchmark

Profile: 500 VU WebSocket order-entry stress test using `tests/load/ws-order-control-test.js`.

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

Full report: [`Trading Platform/docs/BENCHMARK_REPORT.md`](Trading%20Platform/docs/BENCHMARK_REPORT.md)

## Runtime Requirements

| Dependency | Version / Notes |
| --- | --- |
| Java | Java 21 or newer |
| Maven | Maven 3.9+ |
| k6 | Optional, required for load tests |
| Docker Desktop | Optional, for PostgreSQL / Prometheus / Grafana workflows |
| PostgreSQL | Optional audit persistence target |

## Quick Start

```bash
cd "Trading Platform"
./scripts/run-local.sh
```

Open:

```text
Dashboard:             http://localhost:8080
WebSocket orders:      ws://localhost:9090/ws/orders
WebSocket market data: ws://localhost:9090/ws/market-data
```

## Configuration

Create a local environment file:

```bash
cd "Trading Platform"
cp .env.example .env.local
```

Run with local overrides:

```bash
set -a
source .env.local
set +a
./scripts/run-local.sh
```

Common benchmark/runtime variables:

```bash
RISK_OBJECT_POOL_SIZE=262144
MDE_ORDER_PROJECTION_POOL_SIZE=524288
SKIP_STATE_HYDRATION=true
CLEAN_ROOM_ORDERS=false
USE_MAPPED_JOURNAL=false
USE_CHRONICLE_JOURNAL=false
```

Market-data bootstrap variables:

```bash
MARKET_DATA_PROVIDER=simulated
MARKET_DATA_SYMBOLS=AAPL,GOOGL,MSFT,TSLA,AMZN
POLYGON_API_KEY=your_key_here
```

## Run Tests

Run the complete Java suite:

```bash
cd "Trading Platform"
mvn -q test
```

Focused correctness suites:

```bash
mvn -q -Dtest=OrderBookCorrectnessTest test
mvn -q -Dtest=OrderGatewayPipelineTest,CoreRiskEdgeCaseTest test
mvn -q -Dtest=NettyWebSocketServerIntegrationTest test
mvn -q -Dtest=MarketDataEngineTest,ClearingSettlementTest test
```

## Run the WebSocket Load Test

Install k6:

```bash
brew install k6
```

Start the benchmark server:

```bash
cd "Trading Platform"
./scripts/run-benchmark-server.sh
```

In another terminal:

```bash
cd "Trading Platform"
./scripts/run-ws-load-test.sh
```

The benchmark runner preallocates larger risk and market-data pools:

```bash
RISK_OBJECT_POOL_SIZE=262144
MDE_ORDER_PROJECTION_POOL_SIZE=524288
```

## Observability

Start Prometheus and Grafana:

```bash
cd "Trading Platform"
./scripts/run-observability.sh
```

Open:

```text
Prometheus: http://localhost:9091
Grafana:    http://localhost:3000
```

## Project Layout

```text
Trading Platform/
|-- src/
|   |-- exchange/
|   |   |-- core/                 Deterministic runtime, sequencing, matching orchestration
|   |   |-- gateway/              OrderGateway and validation boundary
|   |   |-- risk/                 Client accounts, reservations, pre-trade checks
|   |   |-- dispatch/             Event dispatcher, mutable event batches, ring-buffer events
|   |   |-- marketdata/           L2, trade tape, OHLCV, market-data egress
|   |   |-- journal/              command journals and replay infrastructure
|   |   |-- ws/                   Netty WebSocket gateway and codecs
|   |   |-- clearing/             event-driven post-trade clearing
|   |   `-- replication/          active-passive replication groundwork
|   |-- sim/                      market-maker and liquidity-taker simulators
|   |-- web/                      Javalin dashboard/API shell
|   `-- test/                     JUnit integration and correctness suites
|-- tests/
|   |-- e2e/                      Playwright UI/network resiliency tests
|   `-- load/                     k6 HTTP and WebSocket load profiles
|-- scripts/                      local, benchmark, and observability runners
|-- docs/                         benchmark and architecture reports
|-- grafana/                      dashboard provisioning
|-- docker-compose.yml            optional infrastructure services
`-- pom.xml                       Maven build definition
```

## Important Boundary

ProTrade X is a simulated exchange venue and research-grade execution core. It is not a broker, not a live trading system, and not connected to a protected SIP/NBBO feed by default. The built-in universe is simulated unless an external market-data provider is configured, and all matching occurs inside this engine.
