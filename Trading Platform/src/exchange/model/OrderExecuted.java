package exchange.model;

import Price.Price;

import java.time.Instant;

public record OrderExecuted(
        long sequenceNumber,
        String orderId,
        String clientId,
        String symbol,
        String contraOrderId,
        String contraClientId,
        Side side,
        Price fillPrice,
        int fillQty,
        int leavesQty,
        int cumQty,
        boolean fullFill,
        long engineInNanos,
        long eventEmittedNanos,
        Instant eventTimestamp) implements ExchangeEvent {
}
