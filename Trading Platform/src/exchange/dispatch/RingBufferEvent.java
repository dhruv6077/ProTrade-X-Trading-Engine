package exchange.dispatch;

import Price.Price;
import exchange.model.AdminEvent;
import exchange.model.AdminOperation;
import exchange.model.ExchangeEvent;
import exchange.model.OrderAccepted;
import exchange.model.OrderCancelled;
import exchange.model.OrderExecuted;
import exchange.model.OrderRejected;
import exchange.model.OrderRestated;
import exchange.model.OrderState;
import exchange.model.OrderType;
import exchange.model.RejectReason;
import exchange.model.Side;

import java.time.Instant;

public final class RingBufferEvent {
    public enum EventType {
        ACCEPTED,
        REJECTED,
        EXECUTED,
        CANCELLED,
        RESTATED,
        ADMIN
    }

    private EventType eventType;
    private int batchIndex;
    private int batchSize;
    private long sequenceNumber;
    private Instant eventTimestamp;
    private String orderId;
    private String clientId;
    private String symbol;
    private RejectReason rejectReason;
    private String message;
    private String contraOrderId;
    private String contraClientId;
    private Side side;
    private Price fillPrice;
    private int fillQty;
    private int leavesQty;
    private int cumQty;
    private boolean fullFill;
    private long engineInNanos;
    private long eventEmittedNanos;
    private OrderType orderType;
    private Price price;
    private int quantity;
    private int cancelledQty;
    private AdminOperation adminOperation;

    public void clear() {
        eventType = null;
        batchIndex = 0;
        batchSize = 0;
        sequenceNumber = 0;
        eventTimestamp = null;
        orderId = null;
        clientId = null;
        symbol = null;
        rejectReason = null;
        message = null;
        contraOrderId = null;
        contraClientId = null;
        side = null;
        fillPrice = null;
        fillQty = 0;
        leavesQty = 0;
        cumQty = 0;
        fullFill = false;
        engineInNanos = 0;
        eventEmittedNanos = 0;
        orderType = null;
        price = null;
        quantity = 0;
        cancelledQty = 0;
        adminOperation = null;
    }

    public void copyFrom(ExchangeEvent event, int batchIndex, int batchSize) {
        clear();
        this.batchIndex = batchIndex;
        this.batchSize = batchSize;
        this.sequenceNumber = event.sequenceNumber();
        this.eventTimestamp = event.eventTimestamp();
        this.orderId = event.orderId();
        this.clientId = event.clientId();
        this.symbol = event.symbol();

        if (event instanceof OrderAccepted accepted) {
            this.eventType = EventType.ACCEPTED;
            OrderState order = accepted.order();
            this.side = order.side();
            this.orderType = order.orderType();
            this.price = order.price();
            this.quantity = order.quantity();
            this.leavesQty = order.leavesQty();
            this.cumQty = order.cumQty();
        } else if (event instanceof OrderRejected rejected) {
            this.eventType = EventType.REJECTED;
            this.rejectReason = rejected.reason();
            this.message = rejected.message();
        } else if (event instanceof OrderExecuted executed) {
            this.eventType = EventType.EXECUTED;
            this.contraOrderId = executed.contraOrderId();
            this.contraClientId = executed.contraClientId();
            this.side = executed.side();
            this.fillPrice = executed.fillPrice();
            this.fillQty = executed.fillQty();
            this.leavesQty = executed.leavesQty();
            this.cumQty = executed.cumQty();
            this.fullFill = executed.fullFill();
            this.engineInNanos = executed.engineInNanos();
            this.eventEmittedNanos = executed.eventEmittedNanos();
        } else if (event instanceof OrderCancelled cancelled) {
            this.eventType = EventType.CANCELLED;
            this.cancelledQty = cancelled.cancelledQty();
            this.message = cancelled.reason();
        } else if (event instanceof OrderRestated restated) {
            this.eventType = EventType.RESTATED;
            this.price = restated.price();
            this.quantity = restated.quantity();
            this.leavesQty = restated.leavesQty();
            this.cumQty = restated.cumQty();
        } else if (event instanceof AdminEvent adminEvent) {
            this.eventType = EventType.ADMIN;
            this.adminOperation = adminEvent.operation();
            this.message = adminEvent.message();
        } else {
            throw new IllegalArgumentException("Unsupported exchange event type: " + event.getClass().getName());
        }
    }

    public void copyFrom(RingBufferEvent event, int batchIndex, int batchSize) {
        clear();
        this.eventType = event.eventType;
        this.batchIndex = batchIndex;
        this.batchSize = batchSize;
        this.sequenceNumber = event.sequenceNumber;
        this.eventTimestamp = event.eventTimestamp;
        this.orderId = event.orderId;
        this.clientId = event.clientId;
        this.symbol = event.symbol;
        this.rejectReason = event.rejectReason;
        this.message = event.message;
        this.contraOrderId = event.contraOrderId;
        this.contraClientId = event.contraClientId;
        this.side = event.side;
        this.fillPrice = event.fillPrice;
        this.fillQty = event.fillQty;
        this.leavesQty = event.leavesQty;
        this.cumQty = event.cumQty;
        this.fullFill = event.fullFill;
        this.engineInNanos = event.engineInNanos;
        this.eventEmittedNanos = event.eventEmittedNanos;
        this.orderType = event.orderType;
        this.price = event.price;
        this.quantity = event.quantity;
        this.cancelledQty = event.cancelledQty;
        this.adminOperation = event.adminOperation;
    }

    public void setBatchMetadata(int batchIndex, int batchSize) {
        this.batchIndex = batchIndex;
        this.batchSize = batchSize;
    }

    public void setLatencyNanos(long engineInNanos, long eventEmittedNanos) {
        this.engineInNanos = engineInNanos;
        this.eventEmittedNanos = eventEmittedNanos;
    }

    public void writeAccepted(long sequenceNumber, String orderId, String clientId, String symbol,
            Side side, OrderType orderType, Price price, int quantity, int leavesQty, int cumQty,
            Instant eventTimestamp, int batchIndex, int batchSize) {
        clear();
        this.eventType = EventType.ACCEPTED;
        this.batchIndex = batchIndex;
        this.batchSize = batchSize;
        this.sequenceNumber = sequenceNumber;
        this.eventTimestamp = eventTimestamp;
        this.orderId = orderId;
        this.clientId = clientId;
        this.symbol = symbol;
        this.side = side;
        this.orderType = orderType;
        this.price = price;
        this.quantity = quantity;
        this.leavesQty = leavesQty;
        this.cumQty = cumQty;
    }

    public void writeRejected(long sequenceNumber, String orderId, String clientId, String symbol,
            RejectReason rejectReason, String message, Instant eventTimestamp, int batchIndex, int batchSize) {
        clear();
        this.eventType = EventType.REJECTED;
        this.batchIndex = batchIndex;
        this.batchSize = batchSize;
        this.sequenceNumber = sequenceNumber;
        this.eventTimestamp = eventTimestamp;
        this.orderId = orderId;
        this.clientId = clientId;
        this.symbol = symbol;
        this.rejectReason = rejectReason;
        this.message = message;
    }

    public void writeExecuted(long sequenceNumber, String orderId, String clientId, String symbol,
            String contraOrderId, String contraClientId, Side side, Price fillPrice, int fillQty,
            int leavesQty, int cumQty, boolean fullFill, long engineInNanos, long eventEmittedNanos,
            Instant eventTimestamp, int batchIndex, int batchSize) {
        clear();
        this.eventType = EventType.EXECUTED;
        this.batchIndex = batchIndex;
        this.batchSize = batchSize;
        this.sequenceNumber = sequenceNumber;
        this.eventTimestamp = eventTimestamp;
        this.orderId = orderId;
        this.clientId = clientId;
        this.symbol = symbol;
        this.contraOrderId = contraOrderId;
        this.contraClientId = contraClientId;
        this.side = side;
        this.fillPrice = fillPrice;
        this.fillQty = fillQty;
        this.leavesQty = leavesQty;
        this.cumQty = cumQty;
        this.fullFill = fullFill;
        this.engineInNanos = engineInNanos;
        this.eventEmittedNanos = eventEmittedNanos;
    }

    public void writeCancelled(long sequenceNumber, String orderId, String clientId, String symbol,
            int cancelledQty, String message, Instant eventTimestamp, int batchIndex, int batchSize) {
        clear();
        this.eventType = EventType.CANCELLED;
        this.batchIndex = batchIndex;
        this.batchSize = batchSize;
        this.sequenceNumber = sequenceNumber;
        this.eventTimestamp = eventTimestamp;
        this.orderId = orderId;
        this.clientId = clientId;
        this.symbol = symbol;
        this.cancelledQty = cancelledQty;
        this.message = message;
    }

    public void writeRestated(long sequenceNumber, String orderId, String clientId, String symbol,
            Price price, int quantity, int leavesQty, int cumQty, Instant eventTimestamp,
            int batchIndex, int batchSize) {
        clear();
        this.eventType = EventType.RESTATED;
        this.batchIndex = batchIndex;
        this.batchSize = batchSize;
        this.sequenceNumber = sequenceNumber;
        this.eventTimestamp = eventTimestamp;
        this.orderId = orderId;
        this.clientId = clientId;
        this.symbol = symbol;
        this.price = price;
        this.quantity = quantity;
        this.leavesQty = leavesQty;
        this.cumQty = cumQty;
    }

    public void writeAdmin(long sequenceNumber, String orderId, String clientId, String symbol,
            AdminOperation adminOperation, String message, Instant eventTimestamp, int batchIndex, int batchSize) {
        clear();
        this.eventType = EventType.ADMIN;
        this.batchIndex = batchIndex;
        this.batchSize = batchSize;
        this.sequenceNumber = sequenceNumber;
        this.eventTimestamp = eventTimestamp;
        this.orderId = orderId;
        this.clientId = clientId;
        this.symbol = symbol;
        this.adminOperation = adminOperation;
        this.message = message;
    }

    public ExchangeEvent toImmutableEvent() {
        return switch (eventType) {
            case ACCEPTED -> new OrderAccepted(sequenceNumber, orderId, clientId, symbol,
                    new OrderState(orderId, clientId, symbol, side, orderType, price, quantity, leavesQty, cumQty,
                            sequenceNumber),
                    eventTimestamp);
            case REJECTED -> new OrderRejected(sequenceNumber, orderId, clientId, symbol, rejectReason, message,
                    eventTimestamp);
            case EXECUTED -> new OrderExecuted(sequenceNumber, orderId, clientId, symbol, contraOrderId,
                    contraClientId, side, fillPrice, fillQty, leavesQty, cumQty, fullFill, engineInNanos,
                    eventEmittedNanos, eventTimestamp);
            case CANCELLED -> new OrderCancelled(sequenceNumber, orderId, clientId, symbol, cancelledQty, message,
                    eventTimestamp);
            case RESTATED -> new OrderRestated(sequenceNumber, orderId, clientId, symbol, price, quantity, leavesQty,
                    cumQty, eventTimestamp);
            case ADMIN -> new AdminEvent(sequenceNumber, orderId, clientId, symbol, adminOperation, message,
                    eventTimestamp);
        };
    }

    public EventType getEventType() {
        return eventType;
    }

    public int getBatchIndex() {
        return batchIndex;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public long getSequenceNumber() {
        return sequenceNumber;
    }

    public Instant getEventTimestamp() {
        return eventTimestamp;
    }

    public String getOrderId() {
        return orderId;
    }

    public String getClientId() {
        return clientId;
    }

    public String getSymbol() {
        return symbol;
    }

    public RejectReason getRejectReason() {
        return rejectReason;
    }

    public String getMessage() {
        return message;
    }

    public String getContraOrderId() {
        return contraOrderId;
    }

    public String getContraClientId() {
        return contraClientId;
    }

    public Side getSide() {
        return side;
    }

    public Price getFillPrice() {
        return fillPrice;
    }

    public int getFillQty() {
        return fillQty;
    }

    public int getLeavesQty() {
        return leavesQty;
    }

    public int getCumQty() {
        return cumQty;
    }

    public boolean isFullFill() {
        return fullFill;
    }

    public long getEngineInNanos() {
        return engineInNanos;
    }

    public long getEventEmittedNanos() {
        return eventEmittedNanos;
    }

    public OrderType getOrderType() {
        return orderType;
    }

    public Price getPrice() {
        return price;
    }

    public int getQuantity() {
        return quantity;
    }

    public int getCancelledQty() {
        return cancelledQty;
    }

    public AdminOperation getAdminOperation() {
        return adminOperation;
    }
}
