package exchange.model;

import java.time.Instant;

public record OrderCancelled(
        long sequenceNumber,
        String orderId,
        String clientId,
        String symbol,
        int cancelledQty,
        String reason,
        Instant eventTimestamp) implements ExchangeEvent {
}
