package exchange.model;

import Price.Price;

import java.time.Instant;

public final class MutableOrderCommand implements OrderCommand {
    private long sequenceNumber;
    private Instant inboundTimestamp = Instant.EPOCH;
    private String orderId;
    private String clientId;
    private String symbol;
    private int symbolId;
    private Side side;
    private OrderType orderType;
    private Price price;
    private int quantity;
    private SelfTradePreventionMode stpMode;
    private long ingressTimeNs;

    public void populate(String orderId, String clientId, String symbol, Side side, OrderType orderType,
            Price price, int quantity, SelfTradePreventionMode stpMode) {
        populate(orderId, clientId, symbol, side, orderType, price, quantity, stpMode, System.nanoTime());
    }

    public void populate(String orderId, String clientId, String symbol, Side side, OrderType orderType,
            Price price, int quantity, SelfTradePreventionMode stpMode, long ingressTimeNs) {
        populate(orderId, clientId, symbol, 0, side, orderType, price, quantity, stpMode, ingressTimeNs);
    }

    public void populate(String orderId, String clientId, String symbol, int symbolId, Side side, OrderType orderType,
            Price price, int quantity, SelfTradePreventionMode stpMode, long ingressTimeNs) {
        this.sequenceNumber = 0;
        this.inboundTimestamp = Instant.EPOCH;
        this.orderId = orderId;
        this.clientId = clientId;
        this.symbol = symbol;
        this.symbolId = symbolId;
        this.side = side;
        this.orderType = orderType;
        this.price = price;
        this.quantity = quantity;
        this.stpMode = stpMode;
        this.ingressTimeNs = ingressTimeNs;
    }

    public void reset() {
        sequenceNumber = 0;
        inboundTimestamp = Instant.EPOCH;
        orderId = null;
        clientId = null;
        symbol = null;
        symbolId = 0;
        side = null;
        orderType = null;
        price = null;
        quantity = 0;
        stpMode = null;
        ingressTimeNs = 0L;
    }

    @Override
    public CommandType commandType() {
        return CommandType.NEW_ORDER;
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

    public Side side() {
        return side;
    }

    public OrderType orderType() {
        return orderType;
    }

    public Price price() {
        return price;
    }

    public int quantity() {
        return quantity;
    }

    public SelfTradePreventionMode stpMode() {
        return stpMode;
    }

    @Override
    public long ingressTimeNs() {
        return ingressTimeNs;
    }

    @Override
    public MutableOrderCommand withSequencing(long sequenceNumber, Instant inboundTimestamp) {
        this.sequenceNumber = sequenceNumber;
        this.inboundTimestamp = inboundTimestamp;
        return this;
    }
}
