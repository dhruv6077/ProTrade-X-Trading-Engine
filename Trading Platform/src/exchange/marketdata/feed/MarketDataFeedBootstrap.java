package exchange.marketdata.feed;

import User.ProductManager;
import exchange.core.ExchangeRuntime;
import exchange.model.Side;
import exchange.risk.InMemoryRiskEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public final class MarketDataFeedBootstrap {
    private static final Logger logger = LoggerFactory.getLogger(MarketDataFeedBootstrap.class);

    private MarketDataFeedBootstrap() {
    }

    public static List<String> bootstrapConfiguredFeed(
            ExchangeRuntime runtime,
            ProductManager productManager,
            List<String> fallbackSymbols) {
        String providerName = System.getenv().getOrDefault("MARKET_DATA_PROVIDER", "simulated").trim();
        List<String> requestedSymbols = configuredSymbols(fallbackSymbols);
        if (!"polygon".equalsIgnoreCase(providerName)) {
            registerSymbols(runtime, productManager, requestedSymbols);
            return requestedSymbols;
        }

        String apiKey = System.getenv("POLYGON_API_KEY");
        try (MarketDataProvider provider = new PolygonSnapshotMarketDataProvider(apiKey)) {
            List<MarketDataQuote> quotes = provider.loadQuotes(requestedSymbols);
            if (quotes.isEmpty()) {
                logger.warn("Polygon returned no usable bid/ask snapshots; falling back to simulated symbols");
                registerSymbols(runtime, productManager, requestedSymbols);
                return requestedSymbols;
            }
            InMemoryRiskEngine riskEngine = runtime.riskEngine();
            ArrayList<String> symbols = new ArrayList<>(quotes.size());
            for (MarketDataQuote quote : quotes) {
                symbols.add(quote.symbol());
                registerSymbol(runtime, productManager, quote.symbol());
                riskEngine.setReferencePrice(quote.symbol(), Side.BUY, quote.bidCents());
                riskEngine.setReferencePrice(quote.symbol(), Side.SELL, quote.askCents());
            }
            logger.info("Loaded {} external Polygon market-data snapshot(s)", symbols.size());
            return List.copyOf(symbols);
        } catch (Exception e) {
            logger.warn("External market-data feed unavailable; falling back to simulated symbols: {}", e.getMessage());
            registerSymbols(runtime, productManager, requestedSymbols);
            return requestedSymbols;
        }
    }

    private static List<String> configuredSymbols(List<String> fallbackSymbols) {
        String configured = System.getenv("MARKET_DATA_SYMBOLS");
        if (configured == null || configured.isBlank()) {
            return List.copyOf(fallbackSymbols);
        }
        ArrayList<String> symbols = new ArrayList<>();
        for (String token : configured.split(",")) {
            String symbol = token.trim().toUpperCase(java.util.Locale.ROOT);
            if (!symbol.isBlank()) {
                symbols.add(symbol);
            }
        }
        return symbols.isEmpty() ? List.copyOf(fallbackSymbols) : List.copyOf(symbols);
    }

    private static void registerSymbols(ExchangeRuntime runtime, ProductManager productManager, List<String> symbols) {
        for (String symbol : symbols) {
            registerSymbol(runtime, productManager, symbol);
        }
    }

    private static void registerSymbol(ExchangeRuntime runtime, ProductManager productManager, String symbol) {
        runtime.addSymbol(symbol);
        try {
            productManager.addProduct(symbol);
        } catch (Exception ignored) {
            // ProductManager treats duplicate product registration as harmless.
        }
    }
}
