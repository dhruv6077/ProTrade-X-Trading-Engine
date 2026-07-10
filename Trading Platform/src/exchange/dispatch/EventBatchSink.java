package exchange.dispatch;

import Price.Price;
import exchange.model.AdminOperation;
import exchange.model.ExchangeEvent;
import exchange.model.OrderType;
import exchange.model.RejectReason;
import exchange.model.Side;

import java.time.Instant;

public interface EventBatchSink {
    boolean add(ExchangeEvent event);

    void addAccepted(long sequenceNumber, String orderId, String clientId, String symbol,
            Side side, OrderType orderType, Price price, int quantity, int leavesQty, int cumQty,
            Instant eventTimestamp);

    void addRejected(long sequenceNumber, String orderId, String clientId, String symbol,
            RejectReason reason, String message, Instant eventTimestamp);

    void addExecuted(long sequenceNumber, String orderId, String clientId, String symbol,
            String contraOrderId, String contraClientId, Side side, Price fillPrice, int fillQty,
            int leavesQty, int cumQty, boolean fullFill, long engineInNanos, long eventEmittedNanos,
            Instant eventTimestamp);

    void addCancelled(long sequenceNumber, String orderId, String clientId, String symbol,
            int cancelledQty, String reason, Instant eventTimestamp);

    void addRestated(long sequenceNumber, String orderId, String clientId, String symbol,
            Price price, int quantity, int leavesQty, int cumQty, Instant eventTimestamp);

    void addAdmin(long sequenceNumber, String orderId, String clientId, String symbol,
            AdminOperation operation, String message, Instant eventTimestamp);
}
