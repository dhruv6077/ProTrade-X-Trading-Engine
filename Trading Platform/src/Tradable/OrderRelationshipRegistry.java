package Tradable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Registry for managing relationships between linked orders (OCO, OSO, etc.).
 * Thread-safe singleton that tracks all active order relationships.
 */
public class OrderRelationshipRegistry {
    private static final Logger logger = LoggerFactory.getLogger(OrderRelationshipRegistry.class);
    private static final OrderRelationshipRegistry instance = new OrderRelationshipRegistry();
    
    private final ConcurrentMap<String, OrderRelationship> relationships = new ConcurrentHashMap<>();

    private OrderRelationshipRegistry() {
    }

    /**
     * Get the singleton instance.
     */
    public static OrderRelationshipRegistry getInstance() {
        return instance;
    }

    /**
     * Register a relationship between two orders.
     * 
     * @param orderId1 First order ID
     * @param orderId2 Second order ID
     * @param linkType Type of relationship (OCO, OSO, etc.)
     */
    public void linkOrders(String orderId1, String orderId2, OrderLinkType linkType) {
        if (orderId1 == null || orderId2 == null || linkType == null) {
            throw new IllegalArgumentException("Order IDs and link type cannot be null");
        }

        if (orderId1.equals(orderId2)) {
            throw new IllegalArgumentException("Cannot link an order to itself");
        }

        OrderRelationship relationship = new OrderRelationship(orderId1, orderId2, linkType);
        
        // Store by both order IDs for quick lookup
        relationships.put(orderId1, relationship);
        relationships.put(orderId2, relationship);
        
        logger.debug("Linked orders: {} <-{}-> {}", orderId1, linkType, orderId2);
    }

    /**
     * Get the relationship for a given order.
     * 
     * @param orderId The order ID to lookup
     * @return The OrderRelationship if found, null otherwise
     */
    public OrderRelationship getRelationship(String orderId) {
        return relationships.get(orderId);
    }

    /**
     * Get the linked order ID for a given order.
     * 
     * @param orderId The order ID to lookup
     * @return The linked order ID if found, null otherwise
     */
    public String getLinkedOrderId(String orderId) {
        OrderRelationship rel = getRelationship(orderId);
        if (rel != null) {
            return rel.getLinkedOrderId(orderId);
        }
        return null;
    }

    /**
     * Deactivate a relationship (typically when one order is filled or cancelled).
     * 
     * @param orderId Order ID involved in the relationship
     */
    public void deactivateRelationship(String orderId) {
        OrderRelationship rel = relationships.get(orderId);
        if (rel != null) {
            rel.deactivate();
            
            // Keep in registry but mark as inactive
            logger.debug("Deactivated relationship for order: {}", orderId);
        }
    }

    /**
     * Remove a relationship from the registry.
     * 
     * @param orderId Order ID involved in the relationship
     */
    public void removeRelationship(String orderId) {
        OrderRelationship rel = relationships.remove(orderId);
        if (rel != null) {
            String linkedId = rel.getLinkedOrderId(orderId);
            relationships.remove(linkedId);
            
            logger.debug("Removed relationship for orders: {} and {}", orderId, linkedId);
        }
    }

    /**
     * Check if an order has an active relationship.
     * 
     * @param orderId Order ID to check
     * @return True if order has an active relationship
     */
    public boolean hasActiveRelationship(String orderId) {
        OrderRelationship rel = relationships.get(orderId);
        return rel != null && rel.isActive();
    }

    /**
     * Clear all relationships (typically on system reset).
     */
    public void clear() {
        int count = relationships.size();
        relationships.clear();
        logger.info("Cleared {} order relationships", count / 2); // Divide by 2 since stored twice
    }

    /**
     * Get statistics about current relationships.
     */
    public RegistryStats getStats() {
        return new RegistryStats(
            relationships.size() / 2, // Each relationship is stored twice
            (int) relationships.values().stream().filter(OrderRelationship::isActive).count() / 2
        );
    }

    public static class RegistryStats {
        public final int totalRelationships;
        public final int activeRelationships;

        public RegistryStats(int total, int active) {
            this.totalRelationships = total;
            this.activeRelationships = active;
        }

        @Override
        public String toString() {
            return String.format("Total: %d, Active: %d", totalRelationships, activeRelationships);
        }
    }
}
