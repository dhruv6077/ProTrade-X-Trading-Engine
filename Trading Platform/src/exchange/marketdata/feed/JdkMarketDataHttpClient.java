package exchange.marketdata.feed;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse.BodyHandlers;
import java.time.Duration;

final class JdkMarketDataHttpClient implements MarketDataHttpClient {
    private final HttpClient client;
    private final Duration timeout;

    JdkMarketDataHttpClient(Duration timeout) {
        this.client = HttpClient.newBuilder()
                .connectTimeout(timeout)
                .build();
        this.timeout = timeout;
    }

    @Override
    public HttpResponse get(URI uri) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder(uri)
                .timeout(timeout)
                .GET()
                .build();
        java.net.http.HttpResponse<String> response = client.send(request, BodyHandlers.ofString());
        return new HttpResponse(response.statusCode(), response.body());
    }
}
