import { expect, type Locator, type Page } from "@playwright/test";

export class TradingConsolePage {
  readonly page: Page;
  readonly connectionStatus: Locator;
  readonly symbolSelect: Locator;
  readonly priceInput: Locator;
  readonly quantityInput: Locator;
  readonly submitButton: Locator;
  readonly formMessage: Locator;
  readonly bidRows: Locator;
  readonly askRows: Locator;

  constructor(page: Page) {
    this.page = page;
    this.connectionStatus = page.locator("#connection-text");
    this.symbolSelect = page.locator("#symbol-select");
    this.priceInput = page.locator("#order-price");
    this.quantityInput = page.locator("#order-qty");
    this.submitButton = page.locator(".submit-order");
    this.formMessage = page.locator("#form-message");
    this.bidRows = page.locator("#bid-ladder .ladder-row");
    this.askRows = page.locator("#ask-ladder .ladder-row");
  }

  async goto() {
    await this.page.goto("/");
    await expect(this.page).toHaveTitle(/ProTrade X ECN Console/);
    await expect(this.page.getByRole("heading", { name: "ProTrade X ECN Console" })).toBeVisible();
  }

  async selectSymbol(symbol: string) {
    await this.symbolSelect.selectOption(symbol);
    await expect(this.symbolSelect).toHaveValue(symbol);
  }

  async chooseSell() {
    await this.page.getByRole("button", { name: "Sell" }).click();
    await expect(this.submitButton).toHaveText(/Submit Sell to Gateway/);
  }

  async fillLimitOrder(price: string, quantity: string) {
    await this.priceInput.fill(price);
    await this.quantityInput.fill(quantity);
  }

  async submit() {
    await this.submitButton.click();
  }

  async expectStreaming() {
    await expect(this.connectionStatus).toHaveText("Streaming");
  }

  async expectReconnecting() {
    await expect(this.connectionStatus).toHaveText("Reconnecting");
  }

  async expectOrderBookRendered() {
    await expect(this.page.locator("#l2-asof")).not.toHaveText("Waiting for market data");
    await expect(this.page.locator("#bid-ladder")).toBeVisible();
    await expect(this.page.locator("#ask-ladder")).toBeVisible();
  }
}
