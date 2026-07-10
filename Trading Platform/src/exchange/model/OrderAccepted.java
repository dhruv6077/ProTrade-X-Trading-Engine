package exchange.model;

import java.time.Instant;

public record OrderAccepted(
        long sequenceNumber,
        String orderId,
        String clientId,
        String symbol,
        OrderState order,
        Instant eventTimestamp) implements ExchangeEvent {
}
