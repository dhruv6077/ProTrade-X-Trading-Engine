import http from "k6/http";
import ws from "k6/ws";
import exec from "k6/execution";
import { check } from "k6";
import { Counter, Rate, Trend } from "k6/metrics";

/*
 * WebSocket order-entry control test.
 *
 * This bypasses Javalin/Jetty /api/orders and measures the Netty /ws/orders
 * round-trip from client send to execution-report acknowledgement.
 *
 * Required:
 *   BASE_URL   HTTP app URL for account seeding. Example: http://localhost:8080
 *   WS_URL     WebSocket URL. Example: ws://localhost:9090/ws/orders
 *
 * Useful overrides:
 *   TARGET_VUS=500
 *   TEST_DURATION=60s
 *   ORDER_INTERVAL_MS=40       500 VUs at 40ms is roughly 12.5k sends/sec.
 *   SESSION_MS                 Optional override. Defaults to the active scenario duration,
 *                              so each VU keeps one long-lived WebSocket connection.
 *   CLIENT_POOL_SIZE=500
 *   K6_TRADER_CASH=100000000000000
 */

const BASE_URL = (__ENV.BASE_URL || "http://localhost:8080").replace(/\/+$/, "");
const WS_URL = __ENV.WS_URL || "ws://localhost:9090/ws/orders";
const CLIENT_PREFIX = __ENV.CLIENT_PREFIX || "K6_WS_TRADER";
const SYMBOLS = (__ENV.SYMBOLS || "AAPL,GOOGL,MSFT,TSLA,AMZN")
  .split(",")
  .map((symbol) => symbol.trim())
  .filter(Boolean);
const TARGET_VUS = Number(__ENV.TARGET_VUS || "500");
const WARMUP_VUS = Number(__ENV.WARMUP_VUS || "50");
const WARMUP_DURATION = __ENV.WARMUP_DURATION || "30s";
const RAMP_DURATION = __ENV.RAMP_DURATION || "10s";
const TEST_DURATION = __ENV.TEST_DURATION || "60s";
const CLIENT_POOL_SIZE = Number(__ENV.CLIENT_POOL_SIZE || String(TARGET_VUS));
const K6_TRADER_CASH = Number(__ENV.K6_TRADER_CASH || "100000000000000");
const K6_TRADER_POSITION = Number(__ENV.K6_TRADER_POSITION || "0");
const K6_TRADER_SHORT_SELLING =
  String(__ENV.K6_TRADER_SHORT_SELLING || "false").toLowerCase() === "true";
const RANDOMIZE_SIDE = String(__ENV.RANDOMIZE_SIDE || "false").toLowerCase() === "true";
const ORDER_INTERVAL_MS = Number(__ENV.ORDER_INTERVAL_MS || "40");
const ACK_TIMEOUT_MS = Number(__ENV.ACK_TIMEOUT_MS || "5000");
const SESSION_MS_OVERRIDE = __ENV.SESSION_MS ? Number(__ENV.SESSION_MS) : 0;

export const options = {
  scenarios: {
    warmup: {
      executor: "constant-vus",
      vus: WARMUP_VUS,
      duration: WARMUP_DURATION,
      gracefulStop: "5s",
      exec: "websocketOrderSession",
      tags: { phase: "warmup" },
    },
    ws_order_entry_stress: {
      executor: "ramping-vus",
      startTime: WARMUP_DURATION,
      startVUs: 0,
      stages: [
        { duration: RAMP_DURATION, target: TARGET_VUS },
        { duration: TEST_DURATION, target: TARGET_VUS },
      ],
      gracefulStop: "10s",
      exec: "websocketOrderSession",
      tags: { phase: "stress" },
    },
  },
  thresholds: {
    "ws_order_ack_latency_ms{phase:stress,status:accepted}": ["p(99)<50"],
    "ws_order_ack_latency_ms{phase:stress}": ["p(99)<50"],
    "ws_unmatched_message_rate{phase:stress}": ["rate<0.001"],
    "ws_ack_timeout_rate{phase:stress}": ["rate<0.001"],
    "ws_connection_failure_rate{phase:stress}": ["rate<0.001"],
  },
};

const wsAckLatency = new Trend("ws_order_ack_latency_ms", true);
const wsSentOrders = new Counter("ws_order_sent_count");
const wsAckedOrders = new Counter("ws_order_acked_count");
const wsRejectedOrders = new Counter("ws_order_rejected_count");
const wsAckTimeoutRate = new Rate("ws_ack_timeout_rate");
const wsUnmatchedMessageRate = new Rate("ws_unmatched_message_rate");
const wsConnectionFailureRate = new Rate("ws_connection_failure_rate");
const wsLifecycleReports = new Counter("ws_lifecycle_report_count");

const headers = {
  "Content-Type": "application/json",
  "User-Agent": "k6-protrade-x-ws-control/1.0",
};

export function setup() {
  const payload = {
    clientPrefix: CLIENT_PREFIX,
    clientCount: CLIENT_POOL_SIZE,
    cashCents: K6_TRADER_CASH,
    positionShares: K6_TRADER_POSITION,
    shortSellingEnabled: K6_TRADER_SHORT_SELLING,
    symbols: SYMBOLS,
  };

  const response = http.post(`${BASE_URL}/api/load-test/accounts`, JSON.stringify(payload), { headers });
  if (response.status !== 200) {
    throw new Error(`Failed to seed WebSocket test accounts. status=${response.status} body=${response.body}`);
  }
}

export function websocketOrderSession() {
  const phase = exec.scenario.name === "warmup" ? "warmup" : "stress";
  const clientId = `${CLIENT_PREFIX}_${(__VU % CLIENT_POOL_SIZE) + 1}`;
  const pending = {};
  const sessionMs = SESSION_MS_OVERRIDE > 0 ? SESSION_MS_OVERRIDE : scenarioSessionMs(phase);

  const response = ws.connect(WS_URL, { tags: { phase, endpoint: "ws-orders" } }, (socket) => {
    socket.on("open", () => {
      const stopSendingAt = Date.now() + Math.max(0, sessionMs - ACK_TIMEOUT_MS);

      socket.setInterval(() => {
        expirePending(pending, phase);
        if (Date.now() >= stopSendingAt) {
          return;
        }
        const order = randomOrderPayload(clientId);
        pending[order.orderId] = Date.now();
        wsSentOrders.add(1, { phase, symbol: order.symbol, side: order.side, orderType: order.orderType });
        socket.send(JSON.stringify(order));
      }, ORDER_INTERVAL_MS);

      socket.setTimeout(() => {
        socket.close();
      }, sessionMs);
    });

    socket.on("message", (message) => {
      let payload;
      try {
        payload = JSON.parse(message);
      } catch (_error) {
        wsUnmatchedMessageRate.add(true, { phase, reason: "UNPARSEABLE" });
        return;
      }

      if (payload.type !== "execution-report" || !payload.orderId) {
        wsUnmatchedMessageRate.add(true, { phase, reason: payload.type || "UNKNOWN" });
        return;
      }

      const sentAt = pending[payload.orderId];
      if (sentAt === undefined) {
        // The first execution-report for an order is the gateway acknowledgement
        // that measures ingress latency. Later lifecycle reports, such as fills or
        // cancels for an already-acknowledged order, are expected and must not
        // poison the control-test error rate.
        wsLifecycleReports.add(1, { phase, status: payload.status || "unknown" });
        wsUnmatchedMessageRate.add(false, { phase });
        return;
      }

      delete pending[payload.orderId];
      const status = payload.status || "unknown";
      const latencyMs = Date.now() - sentAt;
      wsAckLatency.add(latencyMs, { phase, status });
      wsAckedOrders.add(1, { phase, status });
      wsAckTimeoutRate.add(false, { phase });
      wsUnmatchedMessageRate.add(false, { phase });

      if (status === "rejected") {
        const reason = payload.details && payload.details.reason ? payload.details.reason : "UNKNOWN";
        wsRejectedOrders.add(1, { phase, rejection_reason: reason });
      }
    });

    socket.on("error", () => {});
  });

  wsConnectionFailureRate.add(!response || response.status !== 101, { phase });
  check(response, {
    "websocket upgrade status is 101": (res) => res && res.status === 101,
  });
}

function expirePending(pending, phase) {
  const now = Date.now();
  for (const orderId of Object.keys(pending)) {
    if (now - pending[orderId] > ACK_TIMEOUT_MS) {
      delete pending[orderId];
      wsAckTimeoutRate.add(true, { phase });
    }
  }
}

function scenarioSessionMs(phase) {
  if (phase === "warmup") {
    return parseDurationMs(WARMUP_DURATION);
  }
  return parseDurationMs(RAMP_DURATION) + parseDurationMs(TEST_DURATION);
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

function randomOrderPayload(clientId) {
  const side = RANDOMIZE_SIDE && Math.random() < 0.5 ? "SELL" : "BUY";
  const orderType = Math.random() < 0.78 ? "LIMIT" : "MARKET";
  const symbol = pick(SYMBOLS);
  return {
    type: "new-order",
    orderId: `${CLIENT_PREFIX}-${__VU}-${__ITER}-${Date.now()}-${randomInt(1, 1_000_000)}`,
    clientId,
    symbol,
    side,
    orderType,
    price: orderType === "MARKET" ? null : randomLimitPrice(symbol),
    quantity: randomInt(1, 100),
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

function randomInt(minInclusive, maxInclusive) {
  return Math.floor(Math.random() * (maxInclusive - minInclusive + 1)) + minInclusive;
}

function centsToDollars(cents) {
  return `${Math.trunc(cents / 100)}.${String(cents % 100).padStart(2, "0")}`;
}

function pick(values) {
  return values[randomInt(0, values.length - 1)];
}
