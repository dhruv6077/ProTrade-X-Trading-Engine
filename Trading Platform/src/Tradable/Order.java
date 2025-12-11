package Tradable;

import Exceptions.InvalidPriceOperation;
import Exceptions.InvalidUserInput;
import Price.Price;

public class Order implements Tradable {

    private final String user;
    private final String product;
    private final Price price;
    private final BookSide side;
    private final String id;
    private final int originalVolume;
    private int remainingVolume;
    private int cancelledVolume;
    private int filledVolume;
    private static final String userIdRegex = "[a-zA-Z0-9_]+";
    public static final String productIdRegex = "[a-zA-Z0-9.]*";
    
    // Advanced order fields
    private String orderType = "LIMIT";                    // LIMIT, MARKET, FOK, OCO
    private String linkedOrderId;                         // For OCO orders
    private OrderLinkType linkType = OrderLinkType.STANDALONE;  // Order linking type
    private OrderStatus status = OrderStatus.PENDING;     // Order status
    private long createdTimestamp;                        // Order creation time

    public Order(String user, String product, Price price, int originalVolume, BookSide side)
            throws InvalidUserInput, InvalidPriceOperation {
        if (user == null || user.length() < 3 || user.length() > 20 || !user.matches(userIdRegex)) {
            throw new InvalidUserInput("User.User Code is Invalid (must be 3-20 alphanumeric chars): " + user);
        }

        if (product == null || product.isEmpty() || product.length() > 5 || !product.matches(productIdRegex)) {
            throw new InvalidUserInput("Invalid product input.");
        }

        if (side == null) {
            throw new InvalidUserInput("Bookside cannot be null.");
        }

        if (originalVolume <= 0 || originalVolume >= 10000) {
            throw new InvalidUserInput("The stock input is not in the specified range.");
        }

        this.user = user;
        this.product = product;
        this.price = price;
        this.side = side;
        this.id = user + product + price.toString() + System.nanoTime();
        this.originalVolume = originalVolume;
        this.remainingVolume = originalVolume;
        this.cancelledVolume = 0;
        this.filledVolume = 0;
        this.createdTimestamp = System.currentTimeMillis();
        this.status = OrderStatus.PENDING;

    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public int getCancelledVolume() {
        return cancelledVolume;
    }

    @Override
    public void setCancelledVolume(int newVol) {
        this.cancelledVolume = newVol;
    }

    @Override
    public int getRemainingVolume() {
        return remainingVolume;
    }

    @Override
    public void setRemainingVolume(int newVol) {
        this.remainingVolume = newVol;
    }

    @Override
    public TradableDTO makeTradableDTO() {
        return new TradableDTO(id, user, product, price, side, originalVolume, remainingVolume, cancelledVolume,
                filledVolume);
    }

    @Override
    public Price getPrice() {
        return price;
    }

    @Override
    public void setFilledVolume(int newVol) {
        this.filledVolume = newVol;
    }

    @Override
    public int getFilledVolume() {
        return filledVolume;
    }

    @Override
    public BookSide getSide() {
        return side;
    }

    @Override
    public String getUser() {
        return user;
    }

    @Override
    public String getProduct() {
        return product;
    }

    @Override
    public int getOriginalVolume() {
        return originalVolume;
    }

    @Override
    public String toString() {
        return "%s order: %s %s at %s, Orig Vol: %d, Rem Vol: %d, Fill Vol: %d, CXL Vol: %d, ID: %s".formatted(user,
                side, product, price, originalVolume, remainingVolume, filledVolume, cancelledVolume, id);
    }

    // Advanced order getter and setter methods
    
    public String getOrderType() {
        return orderType;
    }

    public void setOrderType(String orderType) {
        this.orderType = orderType;
    }

    public boolean isFOK() {
        return "FOK".equalsIgnoreCase(orderType);
    }

    public String getLinkedOrderId() {
        return linkedOrderId;
    }

    public void setLinkedOrderId(String linkedOrderId) {
        this.linkedOrderId = linkedOrderId;
    }

    public OrderLinkType getLinkType() {
        return linkType;
    }

    public void setLinkType(OrderLinkType linkType) {
        this.linkType = linkType;
    }

    public OrderStatus getStatus() {
        return status;
    }

    public void setStatus(OrderStatus status) {
        this.status = status;
    }

    public long getCreatedTimestamp() {
        return createdTimestamp;
    }

    public long getOrderQuantity() {
        return remainingVolume;
    }
}
