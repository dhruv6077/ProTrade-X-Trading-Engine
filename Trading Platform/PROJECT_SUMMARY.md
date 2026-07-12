# ProTrade X / Vision Trader - Project Summary

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

- **Virtual Users:** 500
- **Order Interval:** 200 ms
- **Saturation Hold:** 30s
- **Total Orders Sent Across Runs:** 295,294
- **Total Accepted ACKs Across Runs:** 295,294
- **Total Rejected ACKs Across Runs:** 0
- **Total Load-Shed Rejects Across Runs:** 0
- **Average Order Send Rate:** 1,196.21 orders/sec
- **Average Accepted ACK Rate:** 1,196.21 accepted ACKs/sec
- **Average Accepted p95:** 5.33 ms
- **Average Accepted p99:** 19.67 ms
- **Worst Accepted p99:** 31.00 ms
- **Average ACK Timeout Rate:** 0.00000000
- **Worst ACK Timeout Rate:** 0.00000000
- **Average Total GC Pause:** 0.146 ms
- **Worst Total GC Pause:** 0.166 ms

| Run | Accepted p99 (ms) | Accepted p95 (ms) | ACK Timeout Rate | Total GC Pause (ms) | Accepted ACKs | Rejected ACKs | Load-Shed Rejects |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| run-1-20260711-210323 | 18.00 | 5.00 | 0.00000000 | 0.119 | 98294 | 0 | 0 |
| run-2-20260711-210504 | 10.00 | 5.00 | 0.00000000 | 0.166 | 98500 | 0 | 0 |
| run-3-20260711-210645 | 31.00 | 6.00 | 0.00000000 | 0.154 | 98500 | 0 | 0 |

## Operational Notes

The benchmark harness treats any order without an execution-report ACK as a failure. A valid response may be an accepted order, a business rejection, or a load-shed rejection. This is intentional: a deterministic exchange boundary must never leave a client guessing whether an order was processed.

The `RISK_OBJECT_POOL_SIZE`, `MDE_ORDER_PROJECTION_POOL_SIZE`, and `ORDER_BOOK_INITIAL_CAPACITY` settings are part of the benchmark contract. They size the bounded memory structures used during saturation and should be adjusted deliberately when changing the load profile.
