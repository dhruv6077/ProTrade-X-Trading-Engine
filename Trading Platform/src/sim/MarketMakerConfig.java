package sim;

import java.net.URI;
import java.util.List;
import java.util.Objects;

/**
 * Configurable parameters for a WebSocket-native market maker bot.
 */
public record MarketMakerConfig(
        String clientId,
        List<String> symbols,
        long halfSpreadCents,
        int orderSize,
        long defaultMidPriceCents,
        String webSocketHost,
        int webSocketPort,
        long requoteMidMoveCents) {

    public MarketMakerConfig {
        Objects.requireNonNull(clientId, "clientId");
        Objects.requireNonNull(symbols, "symbols");
        if (symbols.isEmpty()) {
            throw new IllegalArgumentException("symbols must not be empty");
        }
        if (halfSpreadCents < 1) {
            throw new IllegalArgumentException("halfSpreadCents must be positive");
        }
        if (orderSize < 1) {
            throw new IllegalArgumentException("orderSize must be positive");
        }
        if (defaultMidPriceCents < 1) {
            throw new IllegalArgumentException("defaultMidPriceCents must be positive");
        }
        Objects.requireNonNull(webSocketHost, "webSocketHost");
        if (webSocketPort < 1 || webSocketPort > 65_535) {
            throw new IllegalArgumentException("webSocketPort must be between 1 and 65535");
        }
        if (requoteMidMoveCents < 1) {
            throw new IllegalArgumentException("requoteMidMoveCents must be positive");
        }
    }

    public URI ordersUri() {
        return URI.create("ws://" + webSocketHost + ":" + webSocketPort + "/ws/orders");
    }

    public URI marketDataUri() {
        return URI.create("ws://" + webSocketHost + ":" + webSocketPort + "/ws/market-data");
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String clientId;
        private List<String> symbols = List.of();
        private long halfSpreadCents = 5L;
        private int orderSize = 50;
        private long defaultMidPriceCents = 10_000L;
        private String webSocketHost = "localhost";
        private int webSocketPort = 9090;
        private long requoteMidMoveCents = 2L;

        public Builder clientId(String clientId) {
            this.clientId = clientId;
            return this;
        }

        public Builder symbols(List<String> symbols) {
            this.symbols = List.copyOf(symbols);
            return this;
        }

        public Builder halfSpreadCents(long halfSpreadCents) {
            this.halfSpreadCents = halfSpreadCents;
            return this;
        }

        public Builder spreadWidthCents(long spreadWidthCents) {
            this.halfSpreadCents = Math.max(1L, spreadWidthCents / 2L);
            return this;
        }

        public Builder orderSize(int orderSize) {
            this.orderSize = orderSize;
            return this;
        }

        public Builder defaultMidPriceCents(long defaultMidPriceCents) {
            this.defaultMidPriceCents = defaultMidPriceCents;
            return this;
        }

        public Builder webSocketHost(String webSocketHost) {
            this.webSocketHost = webSocketHost;
            return this;
        }

        public Builder webSocketPort(int webSocketPort) {
            this.webSocketPort = webSocketPort;
            return this;
        }

        public Builder requoteMidMoveCents(long requoteMidMoveCents) {
            this.requoteMidMoveCents = requoteMidMoveCents;
            return this;
        }

        public MarketMakerConfig build() {
            return new MarketMakerConfig(clientId, symbols, halfSpreadCents, orderSize, defaultMidPriceCents,
                    webSocketHost, webSocketPort, requoteMidMoveCents);
        }
    }
}
