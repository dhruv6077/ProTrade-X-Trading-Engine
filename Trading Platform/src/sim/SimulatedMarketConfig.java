package sim;

import Tradable.BookSide;

import java.util.Locale;

final class SimulatedMarketConfig {
    private SimulatedMarketConfig() {
    }

    static long referenceMidCents(String symbol) {
        return switch (normalize(symbol)) {
            case "AAPL" -> 169_00L;
            case "GOOGL" -> 280_00L;
            case "MSFT" -> 420_00L;
            case "TSLA" -> 250_00L;
            case "AMZN" -> 180_00L;
            default -> 100_00L;
        };
    }

    static long aggressivePriceCents(String symbol, BookSide side) {
        long mid = referenceMidCents(symbol);
        return side == BookSide.BUY ? mid + 1_00L : Math.max(1L, mid - 1_00L);
    }

    private static String normalize(String symbol) {
        return symbol == null ? "" : symbol.trim().toUpperCase(Locale.ROOT);
    }
}
