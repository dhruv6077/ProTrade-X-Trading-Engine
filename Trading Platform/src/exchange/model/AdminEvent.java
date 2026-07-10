package exchange.model;

import java.time.Instant;

public record AdminEvent(
        long sequenceNumber,
        String orderId,
        String clientId,
        String symbol,
        AdminOperation operation,
        String message,
        Instant eventTimestamp) implements ExchangeEvent {
}
