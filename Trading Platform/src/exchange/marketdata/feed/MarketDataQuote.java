package exchange.marketdata.feed;

import java.time.Instant;

public record MarketDataQuote(
        String symbol,
        long bidCents,
        long askCents,
        long lastCents,
        Instant asOf) {

    public MarketDataQuote {
        if (symbol == null || symbol.isBlank()) {
            throw new IllegalArgumentException("symbol is required");
        }
        symbol = symbol.trim().toUpperCase(java.util.Locale.ROOT);
    }

    public boolean hasBidAsk() {
        return bidCents > 0L && askCents > 0L;
    }
}
