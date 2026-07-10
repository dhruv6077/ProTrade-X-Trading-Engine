package exchange.model;

import java.time.Instant;

public record OrderRejected(
        long sequenceNumber,
        String orderId,
        String clientId,
        String symbol,
        RejectReason reason,
        String message,
        Instant eventTimestamp) implements ExchangeEvent {
}
