package exchange.marketdata.feed;

import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PolygonSnapshotMarketDataProviderTest {

    @Test
    void loadsSingleTickerSnapshotIntoScaledCents() throws Exception {
        AtomicReference<URI> requestedUri = new AtomicReference<>();
        MarketDataHttpClient fakeClient = uri -> {
            requestedUri.set(uri);
            return new MarketDataHttpClient.HttpResponse(200, """
                    {
                      "status": "OK",
                      "ticker": {
                        "ticker": "AAPL",
                        "lastQuote": { "p": 189.12, "P": 189.14, "t": 1719864000000 },
                        "lastTrade": { "p": 189.13, "t": 1719864000000 },
                        "day": { "c": 189.10 }
                      }
                    }
                    """);
        };

        PolygonSnapshotMarketDataProvider provider =
                new PolygonSnapshotMarketDataProvider("test-key", "https://example.test", fakeClient);

        List<MarketDataQuote> quotes = provider.loadQuotes(List.of("aapl"));

        assertEquals(1, quotes.size());
        MarketDataQuote quote = quotes.getFirst();
        assertEquals("AAPL", quote.symbol());
        assertEquals(18_912L, quote.bidCents());
        assertEquals(18_914L, quote.askCents());
        assertEquals(18_913L, quote.lastCents());
        assertEquals("/v2/snapshot/locale/us/markets/stocks/tickers/AAPL", requestedUri.get().getPath());
    }
}
