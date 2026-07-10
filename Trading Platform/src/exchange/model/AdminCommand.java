package exchange.model;

import java.time.Instant;

public record AdminCommand(
        long sequenceNumber,
        Instant inboundTimestamp,
        String orderId,
        String clientId,
        String symbol,
        int symbolId,
        AdminOperation operation) implements OrderCommand {

    public AdminCommand(
            long sequenceNumber,
            Instant inboundTimestamp,
            String orderId,
            String clientId,
            String symbol,
            AdminOperation operation) {
        this(sequenceNumber, inboundTimestamp, orderId, clientId, symbol, 0, operation);
    }

    public AdminCommand(
            long sequenceNumber,
            String orderId,
            String clientId,
            String symbol,
            AdminOperation operation) {
        this(sequenceNumber, Instant.EPOCH, orderId, clientId, symbol, 0, operation);
    }

    public AdminCommand(
            long sequenceNumber,
            String orderId,
            String clientId,
            String symbol,
            int symbolId,
            AdminOperation operation) {
        this(sequenceNumber, Instant.EPOCH, orderId, clientId, symbol, symbolId, operation);
    }

    @Override
    public CommandType commandType() {
        return CommandType.ADMIN;
    }

    @Override
    public AdminCommand withSequencing(long sequenceNumber, Instant inboundTimestamp) {
        return new AdminCommand(sequenceNumber, inboundTimestamp, orderId, clientId, symbol, symbolId, operation);
    }

    public AdminCommand withSymbolId(int symbolId) {
        if (this.symbolId == symbolId) {
            return this;
        }
        return new AdminCommand(sequenceNumber, inboundTimestamp, orderId, clientId, symbol, symbolId, operation);
    }
}
