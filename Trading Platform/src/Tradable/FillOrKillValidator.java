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
     * Check if an order is a FOK order.
     */
    public boolean isFOKOrder(Order order) {
        return order.isFOK();
    }

    /**
     * Validate if a FOK order can be fully executed.
     * Simplified validation - in production would check order book liquidity.
     * 
     * @param fokOrder The FOK order to validate
     * @return True if order can be processed, false if should be rejected
     */
    public boolean validateFOK(Order fokOrder) {
        if (!isFOKOrder(fokOrder)) {
            return true;  // Not a FOK order, validation passes
        }
        
        // Simple FOK validation: ensure order has valid volume
        if (fokOrder.getRemainingVolume() <= 0) {
            logger.warn("FOK order {} rejected: invalid volume {}", 
                fokOrder.getId(), fokOrder.getRemainingVolume());
            return false;
        }
        
        logger.debug("FOK order {} validated successfully with volume {}", 
            fokOrder.getId(), fokOrder.getRemainingVolume());
        
        return true;
    }
}
