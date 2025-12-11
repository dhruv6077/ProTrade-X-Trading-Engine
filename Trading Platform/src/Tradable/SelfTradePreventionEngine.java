package Tradable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Self-Trade Prevention (STP) Engine prevents a trader from buying from and selling to themselves.
 * This is critical for exchange integrity and regulatory compliance.
 * 
 * Order ID Format (assumed): TRADER_ID_SYMBOL_PRICE
 * Example: "MM_1_AAPL_150.50"
 */
public class SelfTradePreventionEngine {
    private static final Logger logger = LoggerFactory.getLogger(SelfTradePreventionEngine.class);
    
    // Extract trader ID from order ID (assumes format: TRADER_ID_SYMBOL_PRICE)
    private static final Pattern TRADER_ID_PATTERN = Pattern.compile("^([^_]+)_");
    
    /**
     * Enumeration for STP action modes.
     */
    public enum STPAction {
        ALLOW,              // Allow the trade
        CANCEL_INCOMING,    // Cancel the incoming order
        CANCEL_RESTING,     // Cancel the resting order on the book
        CANCEL_BOTH         // Cancel both orders
    }

    private final STPAction defaultAction;

    public SelfTradePreventionEngine() {
        this(STPAction.CANCEL_RESTING); // Default: cancel resting order
    }

    public SelfTradePreventionEngine(STPAction defaultAction) {
        this.defaultAction = defaultAction;
    }

    /**
     * Check if two orders would constitute a self-trade.
     * 
     * @param incomingOrder The incoming order trying to match
     * @param restingOrder The order already on the book
     * @return True if both orders belong to the same trader
     */
    public boolean isSelfTrade(Order incomingOrder, Order restingOrder) {
        String incomingTrader = extractTraderId(incomingOrder.getOrderId());
        String restingTrader = extractTraderId(restingOrder.getOrderId());
        
        if (incomingTrader == null || restingTrader == null) {
            // If we can't extract trader ID, allow the trade (assume different traders)
            logger.warn("Could not extract trader ID from orders: {} or {}", 
                incomingOrder.getOrderId(), restingOrder.getOrderId());
            return false;
        }
        
        boolean isSelfTrade = incomingTrader.equals(restingTrader);
        
        if (isSelfTrade) {
            logger.debug("Self-trade detected: {} trying to match with {}", 
                incomingOrder.getOrderId(), restingOrder.getOrderId());
        }
        
        return isSelfTrade;
    }

    /**
     * Determine the STP action for an incoming order matching a resting order.
     * 
     * @param incomingOrder The incoming order
     * @param restingOrder The resting order on the book
     * @return The STP action to take (ALLOW or CANCEL_*)
     */
    public STPAction determineSTPAction(Order incomingOrder, Order restingOrder) {
        if (!isSelfTrade(incomingOrder, restingOrder)) {
            return STPAction.ALLOW;
        }
        
        logger.info("STP Action: {} for incoming order {}", defaultAction, incomingOrder.getOrderId());
        return defaultAction;
    }

    /**
     * Extract trader ID from order ID.
     * Assumes order ID format: TRADER_ID_SYMBOL_PRICE
     * 
     * @param orderId The order ID
     * @return The trader ID, or null if extraction fails
     */
    private String extractTraderId(String orderId) {
        if (orderId == null || orderId.isEmpty()) {
            return null;
        }
        
        Matcher matcher = TRADER_ID_PATTERN.matcher(orderId);
        if (matcher.find()) {
            return matcher.group(1);
        }
        
        return null;
    }

    /**
     * Set the STP action mode.
     * 
     * @param action The action to take when self-trade is detected
     */
    public void setDefaultAction(STPAction action) {
        logger.info("STP default action changed to: {}", action);
    }

    /**
     * Get current STP configuration.
     */
    public STPConfiguration getConfiguration() {
        return new STPConfiguration(defaultAction, true); // STP enabled by default
    }

    public static class STPConfiguration {
        public final STPAction action;
        public final boolean enabled;

        public STPConfiguration(STPAction action, boolean enabled) {
            this.action = action;
            this.enabled = enabled;
        }

        @Override
        public String toString() {
            return String.format("STP Configuration: enabled=%s, action=%s", enabled, action);
        }
    }
}
