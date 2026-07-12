import http from "k6/http";
import ws from "k6/ws";
import exec from "k6/execution";
import { check } from "k6";
import { Counter, Rate, Trend } from "k6/metrics";

/*
 * Vision Trader WebSocket saturation benchmark.
 *
 * Purpose:
 *   Drive legitimate order flow through /ws/orders while separating accepted,
 *   rejected, and load-shed ACK latency.
 *
 * Default profile:
 *   400 WebSocket VUs
 *   1 order every 200ms per VU (2,500 orders/sec nominal at 500 VUs)
 *   deterministic BUY/IOC/AAPL payload (terminal lifecycle; no unbounded resting orders)
 *
 * Required server:
 *   ./scripts/run-benchmark-server.sh
 *
 * Useful overrides:
 *   BASE_URL=http://localhost:8080
 *   WS_URL=ws://localhost:9090/ws/orders
 *   TARGET_VUS=400
 *   WARMUP_VUS=20
 *   WARMUP_RAMP_DURATION=5s
 *   WARMUP_DURATION=15s
 *   ORDER_INTERVAL_MS=200
 *   MAX_IN_FLIGHT=1
 *   TEST_DURATION=30s
 *   ACK_TIMEOUT_MS=5000
 *   SLA_MS=50
 *   K6_TRADER_CASH=1000000000000000
 *   K6_TRADER_MAX_POSITION=2000000000
 */

const BASE_URL = (__ENV.BASE_URL || "http://localhost:8080").replace(/\/+$/, "");
const WS_URL = __ENV.WS_URL || "ws://localhost:9090/ws/orders";
const CLIENT_PREFIX = __ENV.CLIENT_PREFIX || "K6_SAT_TRADER";
const SYMBOLS = (__ENV.SYMBOLS || "AAPL,GOOGL,MSFT,TSLA,AMZN")
  .split(",")
  .map((symbol) => symbol.trim())
  .filter(Boolean);

const TARGET_VUS = Number(__ENV.TARGET_VUS || "400");
const WARMUP_VUS = Number(__ENV.WARMUP_VUS || "20");
const WARMUP_RAMP_DURATION = __ENV.WARMUP_RAMP_DURATION || "5s";
const WARMUP_DURATION = __ENV.WARMUP_DURATION || "15s";
const WARMUP_RAMP_DOWN_DURATION = __ENV.WARMUP_RAMP_DOWN_DURATION || "5s";
const WARMUP_TO_SATURATION_GAP = __ENV.WARMUP_TO_SATURATION_GAP || "5s";
const TEST_DURATION = __ENV.TEST_DURATION || "30s";
const RAMP_TO_100_DURATION = __ENV.RAMP_TO_100_DURATION || "5s";
const RAMP_TO_TARGET_DURATION = __ENV.RAMP_TO_TARGET_DURATION || "5s";
const RAMP_DOWN_DURATION = __ENV.RAMP_DOWN_DURATION || "5s";
const CLIENT_POOL_SIZE = Number(__ENV.CLIENT_POOL_SIZE || String(TARGET_VUS));
const K6_TRADER_CASH = Number(__ENV.K6_TRADER_CASH || "1000000000000000");
const K6_TRADER_POSITION = Number(__ENV.K6_TRADER_POSITION || "1000000000");
const K6_TRADER_MAX_POSITION = Number(__ENV.K6_TRADER_MAX_POSITION || "2000000000");
const K6_TRADER_SHORT_SELLING =
  String(__ENV.K6_TRADER_SHORT_SELLING || "true").toLowerCase() === "true";
const RANDOMIZE_SIDE = String(__ENV.RANDOMIZE_SIDE || "false").toLowerCase() === "true";
const ORDER_INTERVAL_MS = Number(__ENV.ORDER_INTERVAL_MS || "200");
const MAX_IN_FLIGHT = Number(__ENV.MAX_IN_FLIGHT || "1");
const ACK_TIMEOUT_MS = Number(__ENV.ACK_TIMEOUT_MS || "5000");
const SLA_MS = Number(__ENV.SLA_MS || "50");
const SESSION_MS_OVERRIDE = __ENV.SESSION_MS ? Number(__ENV.SESSION_MS) : 0;
const SMOKE_LOG_RESPONSES = Number(__ENV.SMOKE_LOG_RESPONSES || "0");
const SMOKE_SYMBOL = __ENV.SMOKE_SYMBOL || "AAPL";
const SMOKE_SIDE = __ENV.SMOKE_SIDE || "BUY";
const SMOKE_ORDER_TYPE = __ENV.SMOKE_ORDER_TYPE || "IOC";
const SMOKE_PRICE = __ENV.SMOKE_PRICE || "150.00";
const SMOKE_QUANTITY = Number(__ENV.SMOKE_QUANTITY || "1");

let loggedResponses = 0;
let loggedPayloads = 0;

export const options = {
  scenarios: {
    warmup: {
      executor: "ramping-vus",
      startVUs: 0,
      stages: [
        { duration: WARMUP_RAMP_DURATION, target: Math.min(WARMUP_VUS, TARGET_VUS) },
        { duration: WARMUP_DURATION, target: Math.min(WARMUP_VUS, TARGET_VUS) },
        { duration: WARMUP_RAMP_DOWN_DURATION, target: 0 },
      ],
      gracefulRampDown: "10s",
      gracefulStop: "10s",
      exec: "warmupOrderSession",
      tags: { phase: "warmup" },
    },
    saturation: {
      executor: "ramping-vus",
      startTime: saturationStartTime(),
      startVUs: 0,
      stages: [
        { duration: RAMP_TO_100_DURATION, target: Math.min(100, TARGET_VUS) },
        { duration: RAMP_TO_TARGET_DURATION, target: TARGET_VUS },
        { duration: TEST_DURATION, target: TARGET_VUS },
        { duration: RAMP_DOWN_DURATION, target: 0 },
      ],
      gracefulRampDown: "10s",
      gracefulStop: "10s",
      exec: "websocketOrderSession",
      tags: { phase: "saturation" },
    },
  },
  summaryTrendStats: ["avg", "min", "med", "max", "p(90)", "p(95)", "p(99)"],
  thresholds: {
    "accepted_ack_latency_ms{phase:saturation}": ["p(99)<50"],
    "ws_connection_failure_rate{phase:saturation}": ["rate==0"],
    "ack_timeout_rate{phase:saturation}": ["rate==0"],
    "unmatched_message_rate{phase:saturation}": ["rate<0.001"],
  },
};

const acceptedAckLatency = new Trend("accepted_ack_latency_ms", true);
const rejectedAckLatency = new Trend("rejected_ack_latency_ms", true);
const allAckLatency = new Trend("all_ack_latency_ms", true);

const sentOrders = new Counter("orders_sent_total");
const acceptedAcks = new Counter("acks_accepted_total");
const rejectedAcks = new Counter("acks_rejected_total");
const loadShedRejects = new Counter("load_shed_rejects_total");
const lifecycleReports = new Counter("lifecycle_reports_total");
const inFlightSkips = new Counter("inflight_backpressure_skips_total");
const slaBreaches = new Counter("ack_sla_breaches_total");

const ackTimeoutRate = new Rate("ack_timeout_rate");
const unmatchedMessageRate = new Rate("unmatched_message_rate");
const connectionFailureRate = new Rate("ws_connection_failure_rate");
const ackSlaBreachRate = new Rate("ack_sla_breach_rate");

const headers = {
  "Content-Type": "application/json",
  "User-Agent": "k6-vision-trader-saturation/1.0",
};

export function setup() {
  const payload = {
    clientPrefix: CLIENT_PREFIX,
    clientCount: CLIENT_POOL_SIZE,
    cashCents: K6_TRADER_CASH,
    positionShares: K6_TRADER_POSITION,
    maxPosition: K6_TRADER_MAX_POSITION,
    shortSellingEnabled: K6_TRADER_SHORT_SELLING,
    symbols: SYMBOLS,
  };

  const response = http.post(`${BASE_URL}/api/load-test/accounts`, JSON.stringify(payload), { headers });
  if (response.status !== 200) {
    throw new Error(`Account seeding failed: status=${response.status} body=${response.body}`);
  }
}

export function websocketOrderSession() {
  runWebSocketOrderSession("saturation", true);
}

export function warmupOrderSession() {
  runWebSocketOrderSession("warmup", false);
}

function runWebSocketOrderSession(phase, recordMetrics) {
  const clientId = `${CLIENT_PREFIX}_${(__VU % CLIENT_POOL_SIZE) + 1}`;
  const pending = {};
  let pendingCount = 0;
  const sessionMs = SESSION_MS_OVERRIDE > 0 ? SESSION_MS_OVERRIDE : scenarioSessionMs(phase);

  const response = ws.connect(WS_URL, { tags: { phase, endpoint: "ws-orders" } }, (socket) => {
    socket.on("open", () => {
      if (recordMetrics) {
        seedSummaryCounters(phase);
      }
      const drainWindowMs = Math.min(sessionMs, ACK_TIMEOUT_MS + 500);
      const stopSendingAt = Date.now() + Math.max(0, sessionMs - drainWindowMs);

      socket.setInterval(() => {
        pendingCount = expirePending(pending, pendingCount, phase, recordMetrics);
        if (Date.now() >= stopSendingAt) {
          return;
        }
        if (pendingCount >= MAX_IN_FLIGHT) {
          if (recordMetrics) {
            inFlightSkips.add(1, { phase });
          }
          return;
        }

        const order = randomOrderPayload(clientId);
        pending[order.orderId] = Date.now();
        pendingCount++;
        if (recordMetrics) {
          sentOrders.add(1, { phase, symbol: order.symbol, side: order.side, orderType: order.orderType });
        }
        if (recordMetrics && loggedPayloads < SMOKE_LOG_RESPONSES) {
          loggedPayloads++;
          console.log(`[smoke payload ${loggedPayloads}] ${JSON.stringify(order)}`);
        }
        socket.send(JSON.stringify(order));
      }, ORDER_INTERVAL_MS);

      socket.setTimeout(() => socket.close(), sessionMs);
    });

    socket.on("message", (message) => {
      if (recordMetrics && loggedResponses < SMOKE_LOG_RESPONSES) {
        loggedResponses++;
        console.log(`[smoke response ${loggedResponses}] ${message}`);
      }
      let payload;
      try {
        payload = JSON.parse(message);
      } catch (_error) {
        if (recordMetrics) {
          unmatchedMessageRate.add(true, { phase, reason: "UNPARSEABLE" });
        }
        return;
      }

      if (payload.type !== "execution-report" || !payload.orderId) {
        if (recordMetrics) {
          unmatchedMessageRate.add(true, { phase, reason: payload.type || "UNKNOWN" });
        }
        return;
      }

      const sentAt = pending[payload.orderId];
      if (sentAt === undefined) {
        if (recordMetrics) {
          lifecycleReports.add(1, { phase, status: payload.status || "unknown" });
          unmatchedMessageRate.add(false, { phase });
        }
        return;
      }

      delete pending[payload.orderId];
      pendingCount = Math.max(0, pendingCount - 1);
      if (!recordMetrics) {
        return;
      }

      const latencyMs = Date.now() - sentAt;
      const status = payload.status || "unknown";
      allAckLatency.add(latencyMs, { phase, status });
      ackTimeoutRate.add(false, { phase });
      unmatchedMessageRate.add(false, { phase });
      const breachedSla = latencyMs > SLA_MS;
      ackSlaBreachRate.add(breachedSla, { phase, status });
      if (breachedSla) {
        slaBreaches.add(1, { phase, status });
      }

      if (status === "accepted") {
        acceptedAcks.add(1, { phase, status: "accepted" });
        acceptedAckLatency.add(latencyMs, { phase });
        return;
      }

      const reason = rejectionReason(payload);
      const rejectMessage = rejectionMessage(payload);
      const rejectionCategory = isLoadShedReason(`${reason} ${rejectMessage}`) ? "load_shed" : "business_reject";
      rejectedAcks.add(1, { phase, status: "rejected", rejection_reason: reason, category: rejectionCategory });
      rejectedAckLatency.add(latencyMs, { phase, rejection_reason: reason });
      if (rejectionCategory === "load_shed") {
        loadShedRejects.add(1, { phase, rejection_reason: reason, category: "load_shed" });
      }
    });

    socket.on("error", () => {});
  });

  connectionFailureRate.add(!response || response.status !== 101, { phase });
  check(response, {
    "websocket upgrade status is 101": (res) => res && res.status === 101,
  });
}

function expirePending(pending, pendingCount, phase, recordMetrics) {
  const now = Date.now();
  for (const orderId of Object.keys(pending)) {
    if (now - pending[orderId] > ACK_TIMEOUT_MS) {
      delete pending[orderId];
      pendingCount = Math.max(0, pendingCount - 1);
      if (recordMetrics) {
        ackTimeoutRate.add(true, { phase });
      }
    }
  }
  return pendingCount;
}

function scenarioSessionMs(phase) {
  if (phase === "warmup") {
    return parseDurationMs(WARMUP_RAMP_DURATION)
      + parseDurationMs(WARMUP_DURATION)
      + parseDurationMs(WARMUP_RAMP_DOWN_DURATION);
  }
  return parseDurationMs(RAMP_TO_100_DURATION)
    + parseDurationMs(RAMP_TO_TARGET_DURATION)
    + parseDurationMs(TEST_DURATION)
    + parseDurationMs(RAMP_DOWN_DURATION);
}

function saturationStartTime() {
  return formatDurationMs(
    parseDurationMs(WARMUP_RAMP_DURATION)
      + parseDurationMs(WARMUP_DURATION)
      + parseDurationMs(WARMUP_RAMP_DOWN_DURATION)
      + parseDurationMs(WARMUP_TO_SATURATION_GAP)
  );
}

function formatDurationMs(ms) {
  if (ms % 60_000 === 0) {
    return `${ms / 60_000}m`;
  }
  if (ms % 1_000 === 0) {
    return `${ms / 1_000}s`;
  }
  return `${ms}ms`;
}

function parseDurationMs(value) {
  const text = String(value || "").trim();
  const match = /^(\d+(?:\.\d+)?)(ms|s|m)?$/.exec(text);
  if (!match) {
    throw new Error(`Unsupported duration value: ${value}`);
  }
  const amount = Number(match[1]);
  const unit = match[2] || "ms";
  if (unit === "m") {
    return amount * 60_000;
  }
  if (unit === "s") {
    return amount * 1_000;
  }
  return amount;
}

function rejectionReason(payload) {
  if (payload.details && payload.details.reason) {
    return String(payload.details.reason);
  }
  if (payload.reason) {
    return String(payload.reason);
  }
  if (payload.error_code) {
    return String(payload.error_code);
  }
  return "UNKNOWN";
}

function rejectionMessage(payload) {
  if (payload.details && payload.details.message) {
    return String(payload.details.message);
  }
  if (payload.message) {
    return String(payload.message);
  }
  return "";
}

function isLoadShedReason(reason) {
  return /SATURAT|BACKPRESSURE|RING|QUEUE|BUSY|OVERLOAD|LOAD|FULL|CAPACITY|EXHAUST|TRY_NEXT|TRYNEXT/i.test(reason);
}

function seedSummaryCounters(phase) {
  sentOrders.add(0, { phase });
  acceptedAcks.add(0, { phase });
  acceptedAcks.add(0, { phase, status: "accepted" });
  rejectedAcks.add(0, { phase });
  rejectedAcks.add(0, { phase, status: "rejected" });
  rejectedAcks.add(0, { phase, rejection_reason: "NONE" });
  loadShedRejects.add(0, { phase });
  loadShedRejects.add(0, { phase, category: "load_shed" });
  loadShedRejects.add(0, { phase, rejection_reason: "NONE" });
  lifecycleReports.add(0, { phase });
  inFlightSkips.add(0, { phase });
  slaBreaches.add(0, { phase });
  ackSlaBreachRate.add(false, { phase });
}

function randomOrderPayload(clientId) {
  const side = RANDOMIZE_SIDE && Math.random() < 0.5 ? "SELL" : SMOKE_SIDE;
  const orderType = SMOKE_ORDER_TYPE;
  const symbol = SMOKE_SYMBOL || pick(SYMBOLS);
  return {
    type: "new-order",
    orderId: `${CLIENT_PREFIX}-${__VU}-${__ITER}-${Date.now()}-${randomInt(1, 1_000_000)}`,
    clientId,
    symbol,
    side,
    orderType,
    price: orderType === "MARKET" ? null : SMOKE_PRICE,
    quantity: SMOKE_QUANTITY,
  };
}

function randomLimitPrice(symbol) {
  const baseCentsBySymbol = {
    AAPL: 150_00,
    GOOGL: 280_00,
    MSFT: 420_00,
    TSLA: 250_00,
    AMZN: 180_00,
  };
  const base = baseCentsBySymbol[symbol] || 100_00;
  return centsToDollars(Math.max(1, base + randomInt(-100, 100)));
}

function centsToDollars(cents) {
  return `${Math.trunc(cents / 100)}.${String(cents % 100).padStart(2, "0")}`;
}

function randomInt(minInclusive, maxInclusive) {
  return Math.floor(Math.random() * (maxInclusive - minInclusive + 1)) + minInclusive;
}

function pick(values) {
  return values[randomInt(0, values.length - 1)];
}
