package exchange.marketdata;

import Price.Price;
import exchange.model.Side;

import java.time.Instant;

public record TradeRecord(
        long sequenceNumber,
        String symbol,
        Price price,
        int quantity,
        Instant timestamp,
        Side takerSide) {
}
