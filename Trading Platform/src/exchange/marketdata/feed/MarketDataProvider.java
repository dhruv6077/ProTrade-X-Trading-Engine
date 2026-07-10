package exchange.marketdata.feed;

import java.io.IOException;
import java.util.List;

public interface MarketDataProvider extends AutoCloseable {
    List<MarketDataQuote> loadQuotes(List<String> symbols) throws IOException, InterruptedException;

    @Override
    default void close() {
    }
}
