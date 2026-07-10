package exchange.model;

import Price.Price;

import java.time.Instant;

public record ModifyOrderCommand(
        long sequenceNumber,
        Instant inboundTimestamp,
        String orderId,
        String clientId,
        String symbol,
        int symbolId,
        Price newPrice,
        int newQuantity) implements OrderCommand {

    public ModifyOrderCommand(
            long sequenceNumber,
            Instant inboundTimestamp,
            String orderId,
            String clientId,
            String symbol,
            Price newPrice,
            int newQuantity) {
        this(sequenceNumber, inboundTimestamp, orderId, clientId, symbol, 0, newPrice, newQuantity);
    }

    public ModifyOrderCommand(
            long sequenceNumber,
            String orderId,
            String clientId,
            String symbol,
            Price newPrice,
            int newQuantity) {
        this(sequenceNumber, Instant.EPOCH, orderId, clientId, symbol, 0, newPrice, newQuantity);
    }

    public ModifyOrderCommand(
            long sequenceNumber,
            String orderId,
            String clientId,
            String symbol,
            int symbolId,
            Price newPrice,
            int newQuantity) {
        this(sequenceNumber, Instant.EPOCH, orderId, clientId, symbol, symbolId, newPrice, newQuantity);
    }

    @Override
    public CommandType commandType() {
        return CommandType.MODIFY_ORDER;
    }

    @Override
    public ModifyOrderCommand withSequencing(long sequenceNumber, Instant inboundTimestamp) {
        return new ModifyOrderCommand(sequenceNumber, inboundTimestamp, orderId, clientId, symbol, symbolId, newPrice,
                newQuantity);
    }

    public ModifyOrderCommand withSymbolId(int symbolId) {
        if (this.symbolId == symbolId) {
            return this;
        }
        return new ModifyOrderCommand(sequenceNumber, inboundTimestamp, orderId, clientId, symbol, symbolId, newPrice,
                newQuantity);
    }
}
