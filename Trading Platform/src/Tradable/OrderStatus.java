package Tradable;

/**
 * Enumeration for order status in the trading system.
 * Tracks the lifecycle of an order from submission to completion.
 */
public enum OrderStatus {
    /**
     * Order has been received and is pending validation.
     */
    PENDING,

    /**
     * Order has been accepted and placed on the order book.
     */
    ACCEPTED,

    /**
     * Order is partially filled (has matched with some but not all quantity).
     */
    PARTIALLY_FILLED,

    /**
     * Order has been fully filled with no remaining quantity.
     */
    FULLY_FILLED,

    /**
     * Order has been cancelled by the user or system.
     */
    CANCELLED,

    /**
     * Order was cancelled as part of an OCO (One-Cancels-Other) relationship.
     */
    CANCELLED_OCO,

    /**
     * Order was rejected because it's a FOK (Fill-or-Kill) but insufficient liquidity exists.
     */
    REJECTED_FOK,

    /**
     * Order was rejected due to validation failure.
     */
    REJECTED,

    /**
     * Order is waiting for a triggered condition (e.g., One-Triggers-Other).
     */
    TRIGGERED,

    /**
     * Order was cancelled due to self-trade prevention.
     */
    CANCELLED_STP;

    /**
     * Check if the order is in a final state (no further changes expected).
     */
    public boolean isFinal() {
        return this == FULLY_FILLED || 
               this == CANCELLED || 
               this == CANCELLED_OCO || 
               this == REJECTED_FOK || 
               this == REJECTED || 
               this == CANCELLED_STP;
    }

    /**
     * Check if the order is executable (can match on the book).
     */
    public boolean isExecutable() {
        return this == ACCEPTED || this == PARTIALLY_FILLED;
    }
}
