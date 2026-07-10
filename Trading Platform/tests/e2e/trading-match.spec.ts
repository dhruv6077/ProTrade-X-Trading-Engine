import { expect, type Page, type Route, test } from "@playwright/test";
import { TradingConsolePage } from "./pages/TradingConsolePage";

test.describe("trading console precision and L2 rendering", () => {
  test("renders a large L2 snapshot with stable numeric locator assertions", async ({ page }) => {
    await installDeterministicMarketRoutes(page, 250);
    const consolePage = new TradingConsolePage(page);

    await consolePage.goto();
    await consolePage.selectSymbol("AAPL");

    // Locator assertions auto-wait and re-query the DOM, so they are resilient while market
    // numbers are being replaced by rapid stream updates.
    await expect(consolePage.bidRows).toHaveCount(250);
    await expect(consolePage.askRows).toHaveCount(250);
    await expect(consolePage.bidRows.first().locator(".price")).toHaveText("$150.00");
    await expect(consolePage.bidRows.first().locator(".qty")).toHaveText("1,000");
    await expect(consolePage.bidRows.nth(249).locator(".cum")).toHaveText("31,375,000");
    await expect(consolePage.askRows.first().locator(".price")).toHaveText("$150.01");
    await expect(consolePage.askRows.first().locator(".qty")).toHaveText("2,000");
  });

  test("blocks malformed price input and never submits an order request", async ({ page }) => {
    await installDeterministicMarketRoutes(page, 3);
    const consolePage = new TradingConsolePage(page);
    const submittedOrders: string[] = [];

    await page.route("**/api/orders", async (route) => {
      submittedOrders.push(route.request().postData() ?? "");
      await route.fulfill({
        status: 202,
        contentType: "application/json",
        body: JSON.stringify({ accepted: true, message: "Order accepted by gateway", events: [] })
      });
    });

    await consolePage.goto();
    await consolePage.priceInput.fill("abc🚀");
    await consolePage.quantityInput.fill("10");
    await consolePage.submit();

    await expect(consolePage.formMessage).toHaveText("Price must use a one-cent tick, e.g. 100.25.");
    expect(submittedOrders).toHaveLength(0);
  });

  test("rejects over-precision price input before it leaves the browser", async ({ page }) => {
    await installDeterministicMarketRoutes(page, 3);
    const consolePage = new TradingConsolePage(page);
    let orderRequestCount = 0;

    await page.route("**/api/orders", async (route) => {
      orderRequestCount += 1;
      await route.abort("failed");
    });

    await consolePage.goto();
    await consolePage.fillLimitOrder("150.12345", "10");
    await consolePage.submit();

    await expect(consolePage.formMessage).toHaveText("Price must use a one-cent tick, e.g. 100.25.");
    expect(orderRequestCount).toBe(0);
  });

  test("separates simultaneous users into isolated browser contexts", async ({ browser }) => {
    const alice = await browser.newContext();
    const bob = await browser.newContext();
    const alicePage = await alice.newPage();
    const bobPage = await bob.newPage();
    await installDeterministicMarketRoutes(alicePage, 3);
    await installDeterministicMarketRoutes(bobPage, 3);

    const aliceConsole = new TradingConsolePage(alicePage);
    const bobConsole = new TradingConsolePage(bobPage);

    await aliceConsole.goto();
    await bobConsole.goto();
    await aliceConsole.selectSymbol("AAPL");
    await bobConsole.selectSymbol("MSFT");
    await alicePage.locator("#client-id").fill("ALICE_QA");
    await bobPage.locator("#client-id").fill("BOB_QA");

    await expect(aliceConsole.symbolSelect).toHaveValue("AAPL");
    await expect(bobConsole.symbolSelect).toHaveValue("MSFT");
    await expect(alicePage.locator("#client-id")).toHaveValue("ALICE_QA");
    await expect(bobPage.locator("#client-id")).toHaveValue("BOB_QA");

    await alice.close();
    await bob.close();
  });
});

async function installDeterministicMarketRoutes(page: Page, depth: number) {
  await page.route("**/api/symbols", async (route) => {
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify(["AAPL", "MSFT", "AMZN"])
    });
  });
  await page.route("**/api/status", async (route) => {
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({ running: true, eventCount: 42, journalSize: 42 })
    });
  });
  await page.route("**/api/accounts/**", async (route) => {
    await fulfillSse(route, "account", {
      clientId: "WEB_TRADER",
      availableCash: "$100,000.00",
      reservedCash: "$0.00",
      positions: {}
    });
  });
  await page.route("**/api/diagnostics/stream", async (route) => {
    await fulfillSse(route, "metrics", {
      latencyMicros: [2, 4, 3],
      ordersPerSecondBySymbol: { AAPL: 12 },
      shards: [{ symbol: "AAPL", running: true, queuedCommands: 0, completedCommands: 10 }]
    });
  });
  await page.route("**/api/market-data/trades/**", async (route) => {
    await fulfillSse(route, "trades", { trades: [] });
  });
  await page.route("**/api/market-data/l2/**", async (route) => {
    await fulfillSse(route, "snapshot", l2Snapshot(depth));
  });
}

async function fulfillSse(route: Route, event: string, payload: unknown) {
  await route.fulfill({
    status: 200,
    contentType: "text/event-stream",
    headers: {
      "cache-control": "no-cache",
      connection: "keep-alive"
    },
    body: `event: ${event}\ndata: ${JSON.stringify(payload)}\n\n`
  });
}

function l2Snapshot(depth: number) {
  let bidCum = 0;
  let askCum = 0;
  return {
    symbol: "AAPL",
    asOf: "2026-06-28T15:00:00Z",
    bids: Array.from({ length: depth }, (_, index) => {
      const quantity = (index + 1) * 1_000;
      bidCum += quantity;
      return {
        price: formatPrice(15_000 - index),
        quantity,
        cumulativeQuantity: bidCum
      };
    }),
    asks: Array.from({ length: depth }, (_, index) => {
      const quantity = (index + 1) * 2_000;
      askCum += quantity;
      return {
        price: formatPrice(15_001 + index),
        quantity,
        cumulativeQuantity: askCum
      };
    })
  };
}

function formatPrice(cents: number) {
  return `$${Math.trunc(cents / 100).toLocaleString()}.${String(cents % 100).padStart(2, "0")}`;
}
