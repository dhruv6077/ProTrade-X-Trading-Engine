package exchange.marketdata.feed;

import java.io.IOException;
import java.net.URI;

interface MarketDataHttpClient {
    HttpResponse get(URI uri) throws IOException, InterruptedException;

    record HttpResponse(int statusCode, String body) {
    }
}
