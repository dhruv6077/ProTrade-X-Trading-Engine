package exchange.marketdata.feed;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public final class PolygonSnapshotMarketDataProvider implements MarketDataProvider {
    private static final String DEFAULT_BASE_URL = "https://api.polygon.io";

    private final String apiKey;
    private final URI baseUri;
    private final MarketDataHttpClient httpClient;

    public PolygonSnapshotMarketDataProvider(String apiKey) {
        this(apiKey, DEFAULT_BASE_URL, new JdkMarketDataHttpClient(Duration.ofSeconds(5)));
    }

    PolygonSnapshotMarketDataProvider(String apiKey, String baseUrl, MarketDataHttpClient httpClient) {
        if (apiKey == null || apiKey.isBlank()) {
            throw new IllegalArgumentException("POLYGON_API_KEY is required for polygon market data");
        }
        this.apiKey = apiKey.trim();
        this.baseUri = URI.create(baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl);
        this.httpClient = httpClient;
    }

    @Override
    public List<MarketDataQuote> loadQuotes(List<String> symbols) throws IOException, InterruptedException {
        ArrayList<MarketDataQuote> quotes = new ArrayList<>(symbols.size());
        for (String symbol : symbols) {
            MarketDataQuote quote = loadQuote(symbol);
            if (quote != null && quote.hasBidAsk()) {
                quotes.add(quote);
            }
        }
        return List.copyOf(quotes);
    }

    private MarketDataQuote loadQuote(String rawSymbol) throws IOException, InterruptedException {
        String symbol = rawSymbol.trim().toUpperCase(java.util.Locale.ROOT);
        String encodedSymbol = URLEncoder.encode(symbol, StandardCharsets.UTF_8);
        URI uri = baseUri.resolve("/v2/snapshot/locale/us/markets/stocks/tickers/" + encodedSymbol
                + "?apiKey=" + URLEncoder.encode(apiKey, StandardCharsets.UTF_8));
        MarketDataHttpClient.HttpResponse response = httpClient.get(uri);
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new IOException("Polygon snapshot failed for " + symbol + " with HTTP " + response.statusCode());
        }

        JsonObject root = JsonParser.parseString(response.body()).getAsJsonObject();
        JsonObject ticker = object(root, "ticker");
        if (ticker == null) {
            return null;
        }

        JsonObject quote = object(ticker, "lastQuote");
        JsonObject trade = object(ticker, "lastTrade");
        long bid = cents(number(quote, "p"));
        long ask = cents(number(quote, "P"));
        long last = cents(number(trade, "p"));
        if (last <= 0L) {
            last = cents(number(object(ticker, "day"), "c"));
        }
        long updated = longNumber(quote, "t");
        if (updated <= 0L) {
            updated = longNumber(trade, "t");
        }
        return new MarketDataQuote(symbol, bid, ask, last, updated <= 0L ? Instant.now() : Instant.ofEpochMilli(updated));
    }

    private static JsonObject object(JsonObject parent, String name) {
        if (parent == null || !parent.has(name) || !parent.get(name).isJsonObject()) {
            return null;
        }
        return parent.getAsJsonObject(name);
    }

    private static BigDecimal number(JsonObject object, String name) {
        if (object == null || !object.has(name)) {
            return null;
        }
        JsonElement element = object.get(name);
        if (element == null || element.isJsonNull() || !element.isJsonPrimitive()) {
            return null;
        }
        try {
            return element.getAsBigDecimal();
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static long longNumber(JsonObject object, String name) {
        if (object == null || !object.has(name) || object.get(name).isJsonNull()) {
            return 0L;
        }
        try {
            return object.get(name).getAsLong();
        } catch (NumberFormatException e) {
            return 0L;
        }
    }

    private static long cents(BigDecimal dollars) {
        if (dollars == null || dollars.signum() <= 0) {
            return 0L;
        }
        return dollars.movePointRight(2).setScale(0, RoundingMode.HALF_UP).longValue();
    }
}
