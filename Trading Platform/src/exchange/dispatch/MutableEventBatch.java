package exchange.dispatch;

import Price.Price;
import exchange.model.AdminOperation;
import exchange.model.ExchangeEvent;
import exchange.model.OrderType;
import exchange.model.RejectReason;
import exchange.model.Side;

import java.time.Instant;
import java.util.List;

public final class MutableEventBatch implements EventBatchSink {
    private final RingBufferEvent[] events;
    private int size;

    public MutableEventBatch(int capacity) {
        this.events = new RingBufferEvent[capacity];
        for (int i = 0; i < capacity; i++) {
            events[i] = new RingBufferEvent();
        }
    }

    public MutableEventBatch reset() {
        for (int i = 0; i < size; i++) {
            events[i].clear();
        }
        size = 0;
        return this;
    }

    @Override
    public boolean add(ExchangeEvent event) {
        next().copyFrom(event, size, 0);
        size++;
        return true;
    }

    public void addAll(List<ExchangeEvent> immutableEvents) {
        for (int i = 0; i < immutableEvents.size(); i++) {
            add(immutableEvents.get(i));
        }
        updateBatchSize();
    }

    public void copyFrom(MutableEventBatch source) {
        reset();
        for (int i = 0; i < source.size(); i++) {
            next().copyFrom(source.get(i), i, source.size());
            size++;
        }
    }

    public void applyLatency(long engineInNanos, long eventEmittedNanos) {
        for (int i = 0; i < size; i++) {
            RingBufferEvent event = events[i];
            if (event.getEventType() == RingBufferEvent.EventType.EXECUTED) {
                event.setLatencyNanos(engineInNanos, eventEmittedNanos);
            }
        }
    }

    @Override
    public void addAccepted(long sequenceNumber, String orderId, String clientId, String symbol,
            Side side, OrderType orderType, Price price, int quantity, int leavesQty, int cumQty,
            Instant eventTimestamp) {
        next().writeAccepted(sequenceNumber, orderId, clientId, symbol, side, orderType, price, quantity,
                leavesQty, cumQty, eventTimestamp, size, 0);
        size++;
    }

    public void addRejected(long sequenceNumber, String orderId, String clientId, String symbol,
            RejectReason reason, String message, Instant eventTimestamp) {
        next().writeRejected(sequenceNumber, orderId, clientId, symbol, reason, message, eventTimestamp, size, 0);
        size++;
    }

    public void addExecuted(long sequenceNumber, String orderId, String clientId, String symbol,
            String contraOrderId, String contraClientId, Side side, Price fillPrice, int fillQty,
            int leavesQty, int cumQty, boolean fullFill, long engineInNanos, long eventEmittedNanos,
            Instant eventTimestamp) {
        next().writeExecuted(sequenceNumber, orderId, clientId, symbol, contraOrderId, contraClientId, side,
                fillPrice, fillQty, leavesQty, cumQty, fullFill, engineInNanos, eventEmittedNanos,
                eventTimestamp, size, 0);
        size++;
    }

    public void addCancelled(long sequenceNumber, String orderId, String clientId, String symbol,
            int cancelledQty, String reason, Instant eventTimestamp) {
        next().writeCancelled(sequenceNumber, orderId, clientId, symbol, cancelledQty, reason, eventTimestamp,
                size, 0);
        size++;
    }

    public void addRestated(long sequenceNumber, String orderId, String clientId, String symbol,
            Price price, int quantity, int leavesQty, int cumQty, Instant eventTimestamp) {
        next().writeRestated(sequenceNumber, orderId, clientId, symbol, price, quantity, leavesQty, cumQty,
                eventTimestamp, size, 0);
        size++;
    }

    public void addAdmin(long sequenceNumber, String orderId, String clientId, String symbol,
            AdminOperation operation, String message, Instant eventTimestamp) {
        next().writeAdmin(sequenceNumber, orderId, clientId, symbol, operation, message, eventTimestamp, size, 0);
        size++;
    }

    public void updateBatchSize() {
        for (int i = 0; i < size; i++) {
            events[i].setBatchMetadata(i, size);
        }
    }

    public RingBufferEvent get(int index) {
        if (index < 0 || index >= size) {
            throw new IndexOutOfBoundsException(index);
        }
        return events[index];
    }

    public int size() {
        return size;
    }

    public boolean isEmpty() {
        return size == 0;
    }

    private RingBufferEvent next() {
        if (size == events.length) {
            throw new IllegalStateException("Mutable event batch capacity exceeded");
        }
        return events[size];
    }
}
