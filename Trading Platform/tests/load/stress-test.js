import http from "k6/http";
import { check, sleep } from "k6";
import exec from "k6/execution";
import { Counter, Rate, Trend } from "k6/metrics";

/*
 * ProTrade X order-entry stress test.
 *
 * Required environment variables:
 *   BASE_URL          Target app URL. Example: http://localhost:8080
 *
 * Optional environment variables:
 *   EXPECTED_STATUS   HTTP status considered successful. The current app returns 202
 *                     for accepted orders. Set EXPECTED_STATUS=200 only if you change
 *                     /api/orders to return 200 OK.
 *   CLIENT_PREFIX     Prefix for synthetic clients. Default: K6_TRADER
 *   SYMBOLS           Comma-separated symbols. Default: AAPL,GOOGL,MSFT,TSLA,AMZN
 *   ORDER_RATE_SLEEP  Per-iteration sleep in seconds. Default: 0.01
 *   RANDOMIZE_SIDE    Set true only when test accounts have sell inventory or short permission.
 *   EXPECT_JSON       Set false for clean-room no-op endpoints that return text/plain.
 *   CLIENT_POOL_SIZE  Number of synthetic accounts to rotate through. Default: TARGET_VUS.
 *   K6_TRADER_CASH    Optional cash seed per synthetic account, in cents.
 *   K6_TRADER_POSITION Optional per-symbol inventory seed per synthetic account.
 *   K6_TRADER_SHORT_SELLING Optional true/false short-selling flag for seeded accounts.
 *   WARMUP_VUS        JVM/JIT warm-up load. Default: 50
 *   WARMUP_DURATION   JVM/JIT warm-up duration. Default: 30s
 *   TARGET_VUS        Full stress-test load. Default: 500
 *   RAMP_DURATION     Transition from warm-up to target load. Default: 10s
 *   TEST_DURATION     Full-load measurement duration. Default: 60s
 */

const BASE_URL = (__ENV.BASE_URL || "http://localhost:8080").replace(/\/+$/, "");
const EXPECTED_STATUS = Number(__ENV.EXPECTED_STATUS || "202");
const CLIENT_PREFIX = __ENV.CLIENT_PREFIX || "K6_TRADER";
const SYMBOLS = (__ENV.SYMBOLS || "AAPL,GOOGL,MSFT,TSLA,AMZN")
  .split(",")
  .map((symbol) => symbol.trim())
  .filter(Boolean);
const ORDER_RATE_SLEEP = Number(__ENV.ORDER_RATE_SLEEP || "0.01");
const RANDOMIZE_SIDE = String(__ENV.RANDOMIZE_SIDE || "false").toLowerCase() === "true";
const EXPECT_JSON = String(__ENV.EXPECT_JSON || "true").toLowerCase() === "true";
const WARMUP_VUS = Number(__ENV.WARMUP_VUS || "50");
const WARMUP_DURATION = __ENV.WARMUP_DURATION || "30s";
const TARGET_VUS = Number(__ENV.TARGET_VUS || "500");
const RAMP_DURATION = __ENV.RAMP_DURATION || "10s";
const TEST_DURATION = __ENV.TEST_DURATION || "60s";
const CLIENT_POOL_SIZE = Number(__ENV.CLIENT_POOL_SIZE || String(TARGET_VUS));
const K6_TRADER_CASH = Number(__ENV.K6_TRADER_CASH || "0");
const K6_TRADER_POSITION = Number(__ENV.K6_TRADER_POSITION || "0");
const K6_TRADER_SHORT_SELLING =
  String(__ENV.K6_TRADER_SHORT_SELLING || "false").toLowerCase() === "true";

// Treat expected business rejections as successful HTTP exchanges so the built-in
// http_req_failed metric remains focused on transport failures and 5xx errors.
http.setResponseCallback(http.expectedStatuses({ min: 200, max: 499 }));

export const options = {
  scenarios: {
    warmup: {
      executor: "constant-vus",
      vus: WARMUP_VUS,
      duration: WARMUP_DURATION,
      gracefulStop: "5s",
      exec: "submitRandomOrder",
      tags: { phase: "warmup" },
    },
    order_entry_stress: {
      executor: "ramping-vus",
      startTime: WARMUP_DURATION,
      startVUs: 0,
      stages: [
        { duration: RAMP_DURATION, target: TARGET_VUS },
        { duration: TEST_DURATION, target: TARGET_VUS },
      ],
      gracefulStop: "10s",
      exec: "submitRandomOrder",
      tags: { phase: "stress" },
    },
  },
  thresholds: {
    // Gateway SLA: accepted orders only, excluding expected risk/business rejections.
    "http_req_duration{phase:stress,status:202}": ["p(99)<50"],
    "accepted_order_latency_ms{phase:stress}": ["p(99)<50"],
    // A small amount of expected business/risk rejection is allowed.
    "business_rejection_rate{phase:stress}": ["rate<0.30"],
    // Transport failures and 5xx responses should remain rare.
    "unexpected_error_rate{phase:stress}": ["rate<0.01"],
    "http_req_failed{phase:stress}": ["rate<0.01"],
    // Status + response-shape success, without latency folded in.
    "order_success_rate{phase:stress}": ["rate>0.70"],
  },
};

const orderSuccessRate = new Rate("order_success_rate");
const businessRejectionRate = new Rate("business_rejection_rate");
const unexpectedErrorRate = new Rate("unexpected_error_rate");
const rejectedOrders = new Counter("order_rejected_count");
const acceptedOrders = new Counter("order_accepted_count");
const orderSubmitLatency = new Trend("order_submit_latency_ms", true);
const acceptedOrderLatency = new Trend("accepted_order_latency_ms", true);
const rejectedOrderLatency = new Trend("rejected_order_latency_ms", true);

const headers = {
  "Content-Type": "application/json",
  "User-Agent": "k6-protrade-x-stress-test/1.0",
};

export function setup() {
  if (K6_TRADER_CASH <= 0 && K6_TRADER_POSITION <= 0 && !K6_TRADER_SHORT_SELLING) {
    return;
  }

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
    throw new Error(
      `Failed to seed load-test accounts. status=${response.status} body=${response.body}`,
    );
  }
}

export function submitRandomOrder() {
  const payload = randomOrderPayload();
  const phase = exec.scenario.name === "warmup" ? "warmup" : "stress";
  const response = http.post(`${BASE_URL}/api/orders`, JSON.stringify(payload), {
    headers,
    tags: {
      endpoint: "order-entry",
      phase,
      orderType: payload.orderType,
      side: payload.side,
      symbol: payload.symbol,
    },
  });

  orderSubmitLatency.add(response.timings.duration, { phase });

  const statusOk = response.status === EXPECTED_STATUS;
  const jsonOk = !EXPECT_JSON || String(response.headers["Content-Type"] || "").includes("application/json");
  const businessRejected = response.status >= 400 && response.status < 500;
  const unexpectedError = response.status === 0 || response.status >= 500;

  check(response, {
    [`status is ${EXPECTED_STATUS}`]: (res) => res.status === EXPECTED_STATUS,
    "latency is under 50ms": (res) => res.timings.duration < 50,
    "response is JSON": (res) =>
      !EXPECT_JSON || String(res.headers["Content-Type"] || "").includes("application/json"),
  });

  orderSuccessRate.add(statusOk && jsonOk, { phase });
  businessRejectionRate.add(businessRejected, { phase });
  unexpectedErrorRate.add(unexpectedError, { phase });

  if (statusOk) {
    acceptedOrders.add(1, { phase });
    acceptedOrderLatency.add(response.timings.duration, { phase });
  } else if (businessRejected) {
    const rejectionReason = extractRejectionReason(response);
    const rejectionTags = { phase, rejection_reason: rejectionReason };
    rejectedOrders.add(1, rejectionTags);
    rejectedOrderLatency.add(response.timings.duration, rejectionTags);
  } else {
    rejectedOrderLatency.add(response.timings.duration, { phase, rejection_reason: "UNEXPECTED_ERROR" });
  }

  sleep(ORDER_RATE_SLEEP);
}

function randomOrderPayload() {
  // Default to BUY so the stress test measures order-entry latency rather than expected
  // sell-side risk rejections for synthetic users with no inventory. Enable RANDOMIZE_SIDE
  // after seeding inventory or granting short-selling permission to k6 clients.
  const side = RANDOMIZE_SIDE && Math.random() < 0.5 ? "SELL" : "BUY";
  const orderType = Math.random() < 0.78 ? "LIMIT" : "MARKET";
  const symbol = pick(SYMBOLS);

  return {
    orderId: `${CLIENT_PREFIX}-${__VU}-${__ITER}-${Date.now()}`,
    clientId: `${CLIENT_PREFIX}_${(__VU % CLIENT_POOL_SIZE) + 1}`,
    symbol,
    side,
    orderType,
    price: orderType === "MARKET" ? null : randomLimitPrice(symbol),
    quantity: randomQuantity(),
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
  const offset = randomInt(-100, 100);
  return centsToDollars(Math.max(1, base + offset));
}

function randomQuantity() {
  return randomInt(1, 100);
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

function extractRejectionReason(response) {
  const data = parseJsonBody(response);
  if (!data) {
    return "UNPARSEABLE_REJECTION";
  }

  const directReason =
    data.error_code ||
    data.errorCode ||
    data.rejectReason ||
    data.rejectionReason ||
    data.reason ||
    data.code;
  if (directReason) {
    return normalizeRejectionReason(directReason);
  }

  const rejectedEvent = Array.isArray(data.events)
    ? data.events.find((event) => event && event.type === "OrderRejected")
    : null;
  if (rejectedEvent) {
    const eventReason =
      rejectedEvent.error_code ||
      rejectedEvent.errorCode ||
      rejectedEvent.rejectReason ||
      rejectedEvent.rejectionReason ||
      rejectedEvent.reason ||
      rejectedEvent.code ||
      parseReasonPrefix(rejectedEvent.message);
    if (eventReason) {
      return normalizeRejectionReason(eventReason);
    }
  }

  return classifyUserFacingMessage(data.message);
}

function parseJsonBody(response) {
  if (!response || !response.body) {
    return null;
  }
  try {
    return JSON.parse(response.body);
  } catch (_) {
    return null;
  }
}

function parseReasonPrefix(message) {
  if (typeof message !== "string") {
    return null;
  }
  const separator = message.indexOf(":");
  if (separator <= 0) {
    return null;
  }
  return message.slice(0, separator);
}

function classifyUserFacingMessage(message) {
  if (typeof message !== "string" || message.trim() === "") {
    return "UNKNOWN_REJECTION";
  }

  const lower = message.toLowerCase();
  if (lower.includes("available cash") || lower.includes("buying power") || lower.includes("cash is insufficient")) {
    return "RISK_BUYING_POWER";
  }
  if (lower.includes("position limit") || lower.includes("available") && lower.includes("position")) {
    return "RISK_POSITION_LIMIT";
  }
  if (lower.includes("notional") || lower.includes("order value")) {
    return "RISK_NOTIONAL_LIMIT";
  }
  if (lower.includes("kill switch") || lower.includes("trading is currently disabled")) {
    return "RISK_KILL_SWITCH";
  }
  if (lower.includes("self") && lower.includes("trade")) {
    return "WOULD_SELF_TRADE";
  }
  if (lower.includes("price") || lower.includes("tick")) {
    return "INVALID_PRICE";
  }
  if (lower.includes("quantity") || lower.includes("positive whole")) {
    return "INVALID_QUANTITY";
  }
  if (lower.includes("symbol")) {
    return "INVALID_SYMBOL";
  }

  return normalizeRejectionReason(message);
}

function normalizeRejectionReason(reason) {
  const normalized = String(reason)
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
  return normalized || "UNKNOWN_REJECTION";
}
