package exchange.model;

import Price.Price;

import java.time.Instant;

public final class MutableModifyOrderCommand implements OrderCommand {
    private long sequenceNumber;
    private Instant inboundTimestamp = Instant.EPOCH;
    private String orderId;
    private String clientId;
    private String symbol;
    private int symbolId;
    private Price newPrice;
    private int newQuantity;

    public void populate(long sequenceNumber, Instant inboundTimestamp, String orderId, String clientId,
            String symbol, int symbolId, Price newPrice, int newQuantity) {
        this.sequenceNumber = sequenceNumber;
        this.inboundTimestamp = inboundTimestamp;
        this.orderId = orderId;
        this.clientId = clientId;
        this.symbol = symbol;
        this.symbolId = symbolId;
        this.newPrice = newPrice;
        this.newQuantity = newQuantity;
    }

    @Override
    public CommandType commandType() {
        return CommandType.MODIFY_ORDER;
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

    public Price newPrice() {
        return newPrice;
    }

    public int newQuantity() {
        return newQuantity;
    }

    @Override
    public MutableModifyOrderCommand withSequencing(long sequenceNumber, Instant inboundTimestamp) {
        this.sequenceNumber = sequenceNumber;
        this.inboundTimestamp = inboundTimestamp;
        return this;
    }
}
