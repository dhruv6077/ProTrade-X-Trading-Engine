package exchange.marketdata;

import Price.Price;

import java.time.Instant;

public record OhlcvCandle(
        String symbol,
        Instant windowStart,
        Instant windowEnd,
        Price open,
        Price high,
        Price low,
        Price close,
        long volume) {
}
