package exchange.model;

import java.time.Instant;

public sealed interface ExchangeEvent permits AdminEvent, OrderAccepted, OrderRejected, OrderExecuted, OrderCancelled, OrderRestated {
    long sequenceNumber();

    String orderId();

    String clientId();

    String symbol();

    Instant eventTimestamp();
}
