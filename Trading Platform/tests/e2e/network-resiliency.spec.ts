import { expect, test } from "@playwright/test";
import { TradingConsolePage } from "./pages/TradingConsolePage";

test.describe("stream resiliency", () => {
  test("shows reconnecting during a client-side network drop and recovers latest snapshot", async ({ browser }) => {
    // browser.newContext() creates an isolated browser profile. In market-data E2E tests this
    // is critical because each simulated trader gets separate cookies, storage, and network state.
    const context = await browser.newContext();
    const page = await context.newPage();
    const consolePage = new TradingConsolePage(page);

    await consolePage.goto();
    await consolePage.expectStreaming();
    await consolePage.expectOrderBookRendered();

    // Playwright's offline mode simulates the browser losing transport connectivity.
    // EventSource/SSE should enter the same reconnect path a real user would see.
    await context.setOffline(true);
    await consolePage.expectReconnecting();

    await context.setOffline(false);
    await page.reload();
    await consolePage.expectStreaming();
    await consolePage.expectOrderBookRendered();

    // Robust locator assertions avoid relying on exact market values, which may update rapidly.
    await expect(page.locator("#l2-asof")).not.toHaveText("Waiting for market data");

    await context.close();
  });

  test("isolates multiple browser users while recovering from a simultaneous stream drop", async ({ browser }) => {
    const alice = await browser.newContext();
    const bob = await browser.newContext();
    const alicePage = new TradingConsolePage(await alice.newPage());
    const bobPage = new TradingConsolePage(await bob.newPage());

    await alicePage.goto();
    await bobPage.goto();
    await alicePage.selectSymbol("AAPL");
    await bobPage.selectSymbol("MSFT");
    await alicePage.expectStreaming();
    await bobPage.expectStreaming();

    await Promise.all([alice.setOffline(true), bob.setOffline(true)]);
    await alicePage.expectReconnecting();
    await bobPage.expectReconnecting();

    await Promise.all([alice.setOffline(false), bob.setOffline(false)]);
    await Promise.all([alicePage.page.reload(), bobPage.page.reload()]);
    await alicePage.expectStreaming();
    await bobPage.expectStreaming();
    await alicePage.expectOrderBookRendered();
    await bobPage.expectOrderBookRendered();

    await alice.close();
    await bob.close();
  });
});
