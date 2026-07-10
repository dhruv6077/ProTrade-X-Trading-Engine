package sim;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import exchange.model.Side;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sim.ws.BotWebSocketClient;

import java.time.Duration;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * WebSocket-native market maker that maintains a two-sided quote around a theoretical mid-price.
 * Connects to {@code /ws/market-data} for L2 updates and {@code /ws/orders} for order entry
 * and execution reports. Replenishes quotes immediately after fills.
 */
public final class MarketMakerBot implements AutoCloseable {
    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(5);
    private static final long BASE_REJECT_BACKOFF_NANOS = Duration.ofMillis(250).toNanos();
    private static final long MAX_REJECT_BACKOFF_NANOS = Duration.ofSeconds(5).toNanos();

    private static final Logger logger = LoggerFactory.getLogger(MarketMakerBot.class);

    private final MarketMakerConfig config;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicLong orderSequence = new AtomicLong();
    private final Map<String, Long> midPriceCentsBySymbol = new ConcurrentHashMap<>();
    private final Map<String, Long> lastQuotedMidCentsBySymbol = new ConcurrentHashMap<>();
    private final Map<String, Object> quoteLocks = new ConcurrentHashMap<>();
    private final Map<QuoteKey, QuoteIntent> workingQuotes = new ConcurrentHashMap<>();
    private final Map<QuoteKey, String> inFlightOrderIds = new ConcurrentHashMap<>();
    private final Map<String, QuoteKey> quoteKeysByOrderId = new ConcurrentHashMap<>();
    private final Map<QuoteKey, Integer> rejectionStreaks = new ConcurrentHashMap<>();
    private final Map<QuoteKey, Long> nextSubmitTimeNanos = new ConcurrentHashMap<>();

    private BotWebSocketClient orderSocket;
    private final Map<String, BotWebSocketClient> marketDataSockets = new ConcurrentHashMap<>();

    public MarketMakerBot(MarketMakerConfig config) {
        this.config = config;
        for (String symbol : config.symbols()) {
            midPriceCentsBySymbol.put(symbol, config.defaultMidPriceCents());
            quoteLocks.put(symbol, new Object());
        }
    }

    public MarketMakerConfig config() {
        return config;
    }

    public void start() throws InterruptedException {
        if (!started.compareAndSet(false, true)) {
            return;
        }

        orderSocket = new BotWebSocketClient(config.ordersUri(), this::onOrderMessage);
        orderSocket.connect(CONNECT_TIMEOUT);

        for (String symbol : config.symbols()) {
            BotWebSocketClient marketSocket = new BotWebSocketClient(config.marketDataUri(),
                    message -> onMarketData(symbol, message));
            marketSocket.connect(CONNECT_TIMEOUT);
            marketSocket.send(subscribePayload(symbol));
            marketDataSockets.put(symbol, marketSocket);
        }

        for (String symbol : config.symbols()) {
            quoteTwoSided(symbol);
        }

        logger.info("Market maker {} started for symbols {} (spread={}c half, size={})",
                config.clientId(), config.symbols(), config.halfSpreadCents(), config.orderSize());
    }

    @Override
    public void close() {
        if (!started.compareAndSet(true, false)) {
            return;
        }
        for (BotWebSocketClient socket : marketDataSockets.values()) {
            socket.close();
        }
        marketDataSockets.clear();
        if (orderSocket != null) {
            orderSocket.close();
            orderSocket = null;
        }
        logger.info("Market maker {} stopped", config.clientId());
    }

    private void onMarketData(String symbol, String message) {
        JsonObject payload = parse(message);
        String type = stringValue(payload, "type");
        if (!"l2".equals(type)) {
            return;
        }

        Long mid = computeMid(payload);
        if (mid != null) {
            midPriceCentsBySymbol.put(symbol, mid);
        }

        boolean bookThin = isBookThin(payload);
        Long lastQuotedMid = lastQuotedMidCentsBySymbol.get(symbol);
        long currentMid = midPriceCentsBySymbol.getOrDefault(symbol, config.defaultMidPriceCents());
        boolean midMoved = lastQuotedMid == null
                || Math.abs(currentMid - lastQuotedMid) >= config.requoteMidMoveCents();

        if (bookThin || midMoved) {
            quoteTwoSided(symbol);
        }
    }

    private void onOrderMessage(String message) {
        JsonObject payload = parse(message);
        if (!"execution-report".equals(stringValue(payload, "type"))) {
            return;
        }

        String status = stringValue(payload, "status");
        if ("executed".equals(status)) {
            String symbol = stringValue(payload, "symbol");
            JsonObject details = payload.getAsJsonObject("details");
            if (details == null || !details.has("side")) {
                return;
            }
            Side side = Side.valueOf(details.get("side").getAsString().trim().toUpperCase(Locale.ROOT));
            QuoteKey key = new QuoteKey(symbol, side);
            if (booleanValue(details, "fullFill")) {
                workingQuotes.remove(key);
            }
            replenishSide(symbol, side);
            return;
        }

        if ("accepted".equals(status)) {
            markAccepted(payload);
            return;
        }

        if ("rejected".equals(status)) {
            markRejected(payload);
        }
    }

    private void quoteTwoSided(String symbol) {
        Object lock = quoteLocks.computeIfAbsent(symbol, ignored -> new Object());
        synchronized (lock) {
            long mid = midPriceCentsBySymbol.getOrDefault(symbol, config.defaultMidPriceCents());
            long bid = Math.max(1L, mid - config.halfSpreadCents());
            long ask = Math.max(bid + 1L, mid + config.halfSpreadCents());
            submitOrder(symbol, Side.BUY, bid, config.orderSize());
            submitOrder(symbol, Side.SELL, ask, config.orderSize());
            lastQuotedMidCentsBySymbol.put(symbol, mid);
        }
    }

    private void replenishSide(String symbol, Side side) {
        Object lock = quoteLocks.computeIfAbsent(symbol, ignored -> new Object());
        synchronized (lock) {
            long mid = midPriceCentsBySymbol.getOrDefault(symbol, config.defaultMidPriceCents());
            long price = side == Side.BUY
                    ? Math.max(1L, mid - config.halfSpreadCents())
                    : Math.max(2L, mid + config.halfSpreadCents());
            submitOrder(symbol, side, price, config.orderSize());
        }
    }

    private void submitOrder(String symbol, Side side, long priceCents, int quantity) {
        BotWebSocketClient socket = orderSocket;
        if (socket == null || !socket.isOpen()) {
            return;
        }
        QuoteKey key = new QuoteKey(symbol, side);
        long now = System.nanoTime();
        Long nextSubmitTime = nextSubmitTimeNanos.get(key);
        if (nextSubmitTime != null && now < nextSubmitTime) {
            logger.debug("{} quote suppressed on {} {} for {}ms after rejection", config.clientId(), symbol, side,
                    TimeUnit.NANOSECONDS.toMillis(nextSubmitTime - now));
            return;
        }

        QuoteIntent currentQuote = workingQuotes.get(key);
        if (currentQuote != null && currentQuote.matches(priceCents, quantity)) {
            return;
        }
        if (inFlightOrderIds.containsKey(key)) {
            return;
        }

        String orderId = config.clientId() + "-" + symbol + "-" + side.name().charAt(0) + "-"
                + orderSequence.incrementAndGet();
        QuoteIntent intent = new QuoteIntent(symbol, side, priceCents, quantity);
        inFlightOrderIds.put(key, orderId);
        quoteKeysByOrderId.put(orderId, key);
        socket.send("""
                {"type":"new-order","orderId":"%s","clientId":"%s","symbol":"%s","side":"%s","orderType":"LIMIT","price":"%s","quantity":%d}
                """.formatted(orderId, config.clientId(), symbol, side.name(), formatPrice(priceCents), quantity));
        workingQuotes.put(key, intent);
    }

    private void markAccepted(JsonObject payload) {
        String orderId = stringValue(payload, "orderId");
        QuoteKey key = quoteKeysByOrderId.remove(orderId);
        if (key == null) {
            return;
        }
        inFlightOrderIds.remove(key);
        rejectionStreaks.remove(key);
        nextSubmitTimeNanos.remove(key);
    }

    private void markRejected(JsonObject payload) {
        String symbol = stringValue(payload, "symbol");
        String orderId = stringValue(payload, "orderId");
        JsonObject details = payload.getAsJsonObject("details");
        String reason = details == null ? "unknown" : stringValue(details, "message");
        QuoteKey key = quoteKeysByOrderId.remove(orderId);
        if (key != null) {
            inFlightOrderIds.remove(key);
            workingQuotes.remove(key);
            int streak = rejectionStreaks.merge(key, 1, Integer::sum);
            long delay = Math.min(MAX_REJECT_BACKOFF_NANOS, BASE_REJECT_BACKOFF_NANOS << Math.min(streak - 1, 5));
            nextSubmitTimeNanos.put(key, System.nanoTime() + delay);
            logger.debug("{} order rejected on {} {}: {} (backoff={}ms)", config.clientId(), key.symbol(), key.side(),
                    reason, TimeUnit.NANOSECONDS.toMillis(delay));
            return;
        }
        logger.debug("{} order rejected on {}: {}", config.clientId(), symbol, reason);
    }

    private static Long computeMid(JsonObject l2Payload) {
        JsonArray bids = l2Payload.getAsJsonArray("bids");
        JsonArray asks = l2Payload.getAsJsonArray("asks");
        Long bestBid = bestPriceCents(bids);
        Long bestAsk = bestPriceCents(asks);
        if (bestBid != null && bestAsk != null) {
            return (bestBid + bestAsk) / 2L;
        }
        if (bestBid != null) {
            return bestBid;
        }
        return bestAsk;
    }

    private static boolean isBookThin(JsonObject l2Payload) {
        JsonArray bids = l2Payload.getAsJsonArray("bids");
        JsonArray asks = l2Payload.getAsJsonArray("asks");
        return bids == null || bids.isEmpty() || asks == null || asks.isEmpty();
    }

    private static Long bestPriceCents(JsonArray levels) {
        if (levels == null || levels.isEmpty()) {
            return null;
        }
        JsonObject top = levels.get(0).getAsJsonObject();
        if (top.has("priceCents")) {
            return top.get("priceCents").getAsLong();
        }
        return null;
    }

    private static String subscribePayload(String symbol) {
        return "{\"type\":\"subscribe\",\"symbol\":\"" + symbol + "\"}";
    }

    private static JsonObject parse(String message) {
        return JsonParser.parseString(message).getAsJsonObject();
    }

    private static String stringValue(JsonObject payload, String key) {
        return payload.has(key) && !payload.get(key).isJsonNull() ? payload.get(key).getAsString() : null;
    }

    private static boolean booleanValue(JsonObject payload, String key) {
        return payload.has(key) && !payload.get(key).isJsonNull() && payload.get(key).getAsBoolean();
    }

    static String formatPrice(long cents) {
        long absCents = Math.abs(cents);
        String sign = cents < 0 ? "-" : "";
        return sign + (absCents / 100) + "." + String.format(Locale.US, "%02d", absCents % 100);
    }

    private record QuoteKey(String symbol, Side side) {
    }

    private record QuoteIntent(String symbol, Side side, long priceCents, int quantity) {
        private boolean matches(long priceCents, int quantity) {
            return this.priceCents == priceCents && this.quantity == quantity;
        }
    }
}
