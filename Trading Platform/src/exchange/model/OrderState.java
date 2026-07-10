package exchange.model;

import Price.Price;

public record OrderState(
        String orderId,
        String clientId,
        String symbol,
        Side side,
        OrderType orderType,
        Price price,
        int quantity,
        int leavesQty,
        int cumQty,
        long sequenceNumber) {

    public OrderState withExecution(int fillQty) {
        return new OrderState(orderId, clientId, symbol, side, orderType, price, quantity,
                leavesQty - fillQty, cumQty + fillQty, sequenceNumber);
    }

    public OrderState withLeavesQty(int newLeavesQty) {
        return new OrderState(orderId, clientId, symbol, side, orderType, price, quantity,
                newLeavesQty, cumQty, sequenceNumber);
    }

    public OrderState restated(Price newPrice, int newQuantity, long newSequenceNumber) {
        int newLeavesQty = Math.max(0, newQuantity - cumQty);
        return new OrderState(orderId, clientId, symbol, side, orderType, newPrice, newQuantity,
                newLeavesQty, cumQty, newSequenceNumber);
    }
}
