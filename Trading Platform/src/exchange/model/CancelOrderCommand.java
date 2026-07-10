package exchange.model;

import java.time.Instant;

public record CancelOrderCommand(
        long sequenceNumber,
        Instant inboundTimestamp,
        String orderId,
        String clientId,
        String symbol,
        int symbolId) implements OrderCommand {

    public CancelOrderCommand(
            long sequenceNumber,
            Instant inboundTimestamp,
            String orderId,
            String clientId,
            String symbol) {
        this(sequenceNumber, inboundTimestamp, orderId, clientId, symbol, 0);
    }

    public CancelOrderCommand(long sequenceNumber, String orderId, String clientId, String symbol) {
        this(sequenceNumber, Instant.EPOCH, orderId, clientId, symbol, 0);
    }

    public CancelOrderCommand(long sequenceNumber, String orderId, String clientId, String symbol, int symbolId) {
        this(sequenceNumber, Instant.EPOCH, orderId, clientId, symbol, symbolId);
    }

    @Override
    public CommandType commandType() {
        return CommandType.CANCEL_ORDER;
    }

    @Override
    public CancelOrderCommand withSequencing(long sequenceNumber, Instant inboundTimestamp) {
        return new CancelOrderCommand(sequenceNumber, inboundTimestamp, orderId, clientId, symbol, symbolId);
    }

    public CancelOrderCommand withSymbolId(int symbolId) {
        if (this.symbolId == symbolId) {
            return this;
        }
        return new CancelOrderCommand(sequenceNumber, inboundTimestamp, orderId, clientId, symbol, symbolId);
    }
}
