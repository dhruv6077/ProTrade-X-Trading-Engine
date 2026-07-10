package exchange.model;

import java.time.Instant;

public final class MutableCancelOrderCommand implements OrderCommand {
    private long sequenceNumber;
    private Instant inboundTimestamp = Instant.EPOCH;
    private String orderId;
    private String clientId;
    private String symbol;
    private int symbolId;

    public void populate(long sequenceNumber, Instant inboundTimestamp, String orderId, String clientId,
            String symbol, int symbolId) {
        this.sequenceNumber = sequenceNumber;
        this.inboundTimestamp = inboundTimestamp;
        this.orderId = orderId;
        this.clientId = clientId;
        this.symbol = symbol;
        this.symbolId = symbolId;
    }

    @Override
    public CommandType commandType() {
        return CommandType.CANCEL_ORDER;
    }

    @Override
    public long sequenceNumber() {
        return sequenceNumber;
    }

    @Override
    public Instant inboundTimestamp() {
        return inboundTimestamp;
    }

    @Override
    public String clientId() {
        return clientId;
    }

    @Override
    public String symbol() {
        return symbol;
    }

    @Override
    public int symbolId() {
        return symbolId;
    }

    public void setSymbolId(int symbolId) {
        this.symbolId = symbolId;
    }

    @Override
    public String orderId() {
        return orderId;
    }

    @Override
    public MutableCancelOrderCommand withSequencing(long sequenceNumber, Instant inboundTimestamp) {
        this.sequenceNumber = sequenceNumber;
        this.inboundTimestamp = inboundTimestamp;
        return this;
    }
}
