package Tradable;

/**
 * Represents the relationship between two linked orders.
 * Used for managing OCO, OSO, and other complex order types.
 */
public class OrderRelationship {
    private final String primaryOrderId;
    private final String linkedOrderId;
    private final OrderLinkType linkType;
    private volatile boolean isActive;
    private long createdTimestamp;

    public OrderRelationship(String primaryOrderId, String linkedOrderId, OrderLinkType linkType) {
        this.primaryOrderId = primaryOrderId;
        this.linkedOrderId = linkedOrderId;
        this.linkType = linkType;
        this.isActive = true;
        this.createdTimestamp = System.currentTimeMillis();
    }

    public String getPrimaryOrderId() {
        return primaryOrderId;
    }

    public String getLinkedOrderId() {
        return linkedOrderId;
    }

    public OrderLinkType getType() {
        return linkType;
    }

    public boolean isActive() {
        return isActive;
    }

    public void deactivate() {
        this.isActive = false;
    }

    public long getCreatedTimestamp() {
        return createdTimestamp;
    }

    /**
     * Check if the given order ID is part of this relationship.
     */
    public boolean involves(String orderId) {
        return primaryOrderId.equals(orderId) || linkedOrderId.equals(orderId);
    }

    /**
     * Get the linked order ID for the given order.
     */
    public String getLinkedOrderId(String orderId) {
        if (primaryOrderId.equals(orderId)) {
            return linkedOrderId;
        } else if (linkedOrderId.equals(orderId)) {
            return primaryOrderId;
        }
        return null;
    }

    @Override
    public String toString() {
        return "OrderRelationship{" +
                "primaryOrderId='" + primaryOrderId + '\'' +
                ", linkedOrderId='" + linkedOrderId + '\'' +
                ", linkType=" + linkType +
                ", isActive=" + isActive +
                '}';
    }
}
