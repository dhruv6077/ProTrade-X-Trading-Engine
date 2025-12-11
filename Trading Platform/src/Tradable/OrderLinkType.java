package Tradable;

/**
 * Enumeration for order linking types in advanced order management.
 * Supports complex order relationships like OCO (One-Cancels-Other).
 */
public enum OrderLinkType {
    /**
     * Order stands alone, no linking with other orders.
     */
    STANDALONE,

    /**
     * One-Cancels-Other: If one order is filled, the other is automatically cancelled.
     * Example: Sell at $150 or Sell at $145, but not both.
     */
    ONE_CANCELS_OTHER,

    /**
     * One-Sends-Other: Filling one order triggers submission of another order.
     * Example: Sell 100 shares, then buy 50 shares at a better price.
     */
    ONE_SENDS_OTHER,

    /**
     * One-Triggers-Other: Filling one order at a specific price triggers another order.
     * Example: Buy at market, then if filled, sell half at $X higher.
     */
    ONE_TRIGGERS_OTHER
}
