package exchange.model;

import Price.Price;

import java.time.Instant;

public record OrderRestated(
        long sequenceNumber,
        String orderId,
        String clientId,
        String symbol,
        Price price,
        int quantity,
        int leavesQty,
        int cumQty,
        Instant eventTimestamp) implements ExchangeEvent {
}
