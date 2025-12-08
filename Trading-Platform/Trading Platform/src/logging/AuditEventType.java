package logging;

/**
 * Enumeration of all audit event types in the trading platform.
 * Each event type represents a significant action that requires audit logging.
 */
public enum AuditEventType {
    // Order Events
    ORDER_PLACED("Order placed in the system"),
    ORDER_CANCELLED("Order cancelled by user or system"),
    ORDER_FILLED("Order fully filled"),
    ORDER_PARTIALLY_FILLED("Order partially filled"),
    
    // Quote Events
    QUOTE_SUBMITTED("Quote submitted to market"),
    QUOTE_CANCELLED("Quote cancelled"),
    
    // Trade Events
    TRADE_EXECUTED("Trade executed between buy and sell orders"),
    
    // Market Events
    MARKET_UPDATE("Market data updated"),
    PRODUCT_ADDED("New product added to system"),
    
    // User Events
    USER_CREATED("New user created"),
    USER_SUBSCRIBED("User subscribed to market data"),
    USER_UNSUBSCRIBED("User unsubscribed from market data"),
    
    // System Events
    SYSTEM_START("Trading system started"),
    SYSTEM_SHUTDOWN("Trading system shutdown");
    
    private final String description;
    
    AuditEventType(String description) {
        this.description = description;
    }
    
    public String getDescription() {
        return description;
    }
}
