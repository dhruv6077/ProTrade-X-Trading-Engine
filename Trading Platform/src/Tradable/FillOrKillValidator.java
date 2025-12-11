package Tradable;

import Exceptions.InvalidPriceOperation;
import Price.Price;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Validator for Fill-or-Kill (FOK) orders.
 * FOK orders must be executed in full immediately, or rejected entirely.
 * No partial fills are allowed.
 */
public class FillOrKillValidator {
    private static final Logger logger = LoggerFactory.getLogger(FillOrKillValidator.class);

    /**
     * Validate if a FOK order can be fully executed against the given book side.
     * 
     * @param fokOrder The FOK order to validate
     * @param oppositeBookSide The opposite side of the order book (where matches occur)
     * @return True if order can be fully filled, false otherwise
     * @throws InvalidPriceOperation If price operations fail
     */
    public boolean validateFOK(Order fokOrder, ProductBookSide oppositeBookSide) 
            throws InvalidPriceOperation {
        
        if (fokOrder.getOrderType() != null && fokOrder.getOrderType().equalsIgnoreCase("FOK")) {
            // Get total available liquidity at acceptable prices
            long availableLiquidity = getAvailableLiquidity(fokOrder, oppositeBookSide);
            
            boolean canFill = availableLiquidity >= fokOrder.getQuantity();
            
            if (!canFill) {
                logger.debug("FOK validation failed for order {}: need {}, available {}", 
                    fokOrder.getOrderId(), fokOrder.getQuantity(), availableLiquidity);
            }
            
            return canFill;
        }
        
        // Non-FOK orders always pass validation
        return true;
    }

    /**
     * Calculate total available liquidity at acceptable prices.
     * For a buy order, collect all shares available at the ask price or better.
     * For a sell order, collect all shares available at the bid price or better.
     * 
     * @param incomingOrder The incoming order
     * @param oppositeBookSide The book side with available liquidity
     * @return Total quantity available for execution
     * @throws InvalidPriceOperation If price operations fail
     */
    private long getAvailableLiquidity(Order incomingOrder, ProductBookSide oppositeBookSide) 
            throws InvalidPriceOperation {
        
        if (oppositeBookSide == null || oppositeBookSide.isEmpty()) {
            return 0;
        }
        
        long totalAvailable = 0;
        
        // Iterate through the book side and accumulate liquidity at acceptable prices
        // We assume the book is ordered by price (best prices first)
        // This is a simplified implementation; actual implementation would iterate through price levels
        
        // For now, just return the total volume available
        // In production, would implement proper price level iteration
        totalAvailable = oppositeBookSide.getTotalVolume();
        
        logger.debug("Available liquidity for FOK order {}: {} shares", 
            incomingOrder.getOrderId(), totalAvailable);
        
        return totalAvailable;
    }

    /**
     * Check if an order is a FOK order.
     * 
     * @param order The order to check
     * @return True if order is FOK type
     */
    public boolean isFOKOrder(Order order) {
        return order != null && 
               order.getOrderType() != null && 
               order.getOrderType().equalsIgnoreCase("FOK");
    }

    /**
     * Get validation statistics.
     */
    public static class FOKValidationStats {
        public long totalFOKOrders;
        public long acceptedFOKOrders;
        public long rejectedFOKOrders;
        public double acceptanceRate;

        public FOKValidationStats(long total, long accepted, long rejected) {
            this.totalFOKOrders = total;
            this.acceptedFOKOrders = accepted;
            this.rejectedFOKOrders = rejected;
            this.acceptanceRate = total > 0 ? (double) accepted / total : 0;
        }

        @Override
        public String toString() {
            return String.format(
                "FOK Stats: Total=%d, Accepted=%d, Rejected=%d, Rate=%.2f%%",
                totalFOKOrders, acceptedFOKOrders, rejectedFOKOrders, acceptanceRate * 100
            );
        }
    }
}
