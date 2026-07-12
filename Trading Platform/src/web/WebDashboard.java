package web;

import Price.Price;
import Price.PriceFactory;
import User.ProductManager;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import exchange.core.ExchangeRuntime;
import exchange.dispatch.EventListener;
import exchange.dispatch.RingBufferEvent;
import exchange.dispatch.RingBufferEventListener;
import exchange.gateway.OrderGateway;
import exchange.marketdata.L2Level;
import exchange.marketdata.L2Snapshot;
import exchange.marketdata.MarketDataEngine;
import exchange.marketdata.OhlcvCandle;
import exchange.marketdata.TradeRecord;
import exchange.model.ExchangeEvent;
import exchange.model.OrderAccepted;
import exchange.model.OrderCancelled;
import exchange.model.OrderExecuted;
import exchange.model.OrderRejected;
import exchange.model.OrderRestated;
import exchange.model.OrderState;
import exchange.model.OrderType;
import exchange.model.RejectReason;
import exchange.model.SelfTradePreventionMode;
import exchange.model.Side;
import exchange.risk.ClientAccount;
import exchange.risk.InMemoryRiskEngine.RestingOrderSnapshot;
import exchange.risk.RiskProfile;
import exchange.telemetry.LatencyTelemetry;
import exchange.ws.NettyWebSocketServer;
import io.javalin.Javalin;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import java.time.Duration;
import io.javalin.http.Context;
import io.javalin.http.staticfiles.Location;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import sim.SimulationManager;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Reader;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

public class WebDashboard {
    private static final Gson GSON = new Gson();
    private static final int L2_DEPTH = 10;
    private static final int TAPE_LIMIT = 60;
    private static final int LATENCY_LIMIT = 120;
    private static final int MARKET_STREAM_INTERVAL_MS = 250;
    private static final int ACCOUNT_STREAM_INTERVAL_MS = 500;
    private static final int METRICS_STREAM_INTERVAL_MS = 1_000;
    private static final String ACCEPTED_ORDER_RESPONSE_JSON =
            "{\"accepted\":true,\"message\":\"Order accepted by gateway\",\"events\":[]}";

    private static Javalin app;
    private static DashboardTelemetry telemetry;
    private static NettyWebSocketServer webSocketServer;
    private static PrometheusMeterRegistry prometheusRegistry;

    public static void start() {
        if (app != null) {
            return;
        }

        SimulationManager.getInstance().initialize();
        ExchangeRuntime runtime = ExchangeRuntime.getInstance();
        telemetry = new DashboardTelemetry();
        runtime.dispatcher().addRingBufferListener(telemetry);
        webSocketServer = new NettyWebSocketServer(runtime, 9090);
        webSocketServer.start();
        prometheusRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        app = Javalin.create(config -> {
            config.staticFiles.add("/public", Location.CLASSPATH);
            config.jetty.threadPool = jettyThreadPool();
            config.jetty.modifyHttpConfiguration(http -> {
                http.setSendServerVersion(false);
                http.setSendDateHeader(false);
            });
            config.jetty.addConnector((server, httpConfiguration) -> {
                int acceptors = intSetting("JETTY_ACCEPTORS", "jetty.acceptors", 2);
                int selectors = intSetting("JETTY_SELECTORS", "jetty.selectors", 4);
                ServerConnector connector = new ServerConnector(server, acceptors, selectors);
                connector.setHost(System.getProperty("jetty.host", System.getenv("JETTY_HOST")));
                connector.setPort(intSetting("JETTY_PORT", "jetty.port", 8080));
                connector.setAcceptQueueSize(intSetting("JETTY_ACCEPT_QUEUE_SIZE", "jetty.acceptQueueSize", 8192));
                connector.setAcceptedTcpNoDelay(true);
                connector.setReuseAddress(true);
                return connector;
            });
        }).start(8080);

        app.get("/metrics", ctx -> {
            ctx.contentType("text/plain; version=0.0.4; charset=utf-8");
            ctx.result(prometheusRegistry.scrape());
        });
        app.get("/api/status", ctx -> json(ctx, statusPayload(runtime)));
        app.get("/api/symbols", ctx -> json(ctx, symbols()));
        app.get("/api/market-data", ctx -> json(ctx, allMarketSnapshots(runtime.marketDataEngine())));
        app.get("/api/market-data/l2/{symbol}", ctx -> streamL2(ctx, runtime.marketDataEngine()));
        app.get("/api/market-data/trades/{symbol}", ctx -> streamTrades(ctx, runtime.marketDataEngine()));
        app.get("/api/accounts/{clientId}", ctx -> streamAccount(ctx, runtime));
        app.get("/api/accounts/{clientId}/snapshot", ctx -> {
            String clientId = ctx.pathParam("clientId").trim();
            json(ctx, accountSnapshot(runtime, clientId));
        });
        app.get("/api/telemetry/latency", ctx -> json(ctx, LatencyTelemetry.getInstance().snapshot()));
        app.get("/api/diagnostics/stream", ctx -> streamDiagnostics(ctx, runtime));
        app.get("/api/ohlcv", ctx -> json(ctx, candles(runtime.marketDataEngine().closedCandles())));
        app.post("/api/load-test/accounts", ctx -> {
            if (!loadTestAdminEnabled()) {
                ctx.status(404).result("Not found");
                return;
            }
            LoadTestAccountsRequest request = GSON.fromJson(ctx.body(), LoadTestAccountsRequest.class);
            LoadTestAccountsResponse response = seedLoadTestAccounts(runtime, request);
            json(ctx, response);
        });

        if (cleanRoomOrdersEnabled()) {
            app.post("/api/orders", ctx -> ctx.status(202).result("Accepted"));
        } else {
            app.post("/api/orders", ctx -> {
                OrderRequest request = parseOrderRequest(ctx.req().getReader());
                submitOrder(ctx, runtime, request);
            });
        }

        app.post("/api/simulation/start", ctx -> {
            try {
                SimulationManager.getInstance().start();
                json(ctx, Map.of("status", "started"));
            } catch (Exception e) {
                ctx.status(500);
                json(ctx, Map.of(
                        "status", "error",
                        "message", "Simulation could not be started: " + e.getMessage()));
            }
        });

        app.post("/api/simulation/stop", ctx -> {
            try {
                SimulationManager.getInstance().stop();
                json(ctx, Map.of("status", "stopped"));
            } catch (Exception e) {
                ctx.status(500);
                json(ctx, Map.of(
                        "status", "error",
                        "message", "Simulation could not be stopped: " + e.getMessage()));
            }
        });

        System.out.println("Web Dashboard started at http://localhost:8080");
        System.out.println("Netty WebSocket API started at ws://localhost:9090");
    }

    private static boolean cleanRoomOrdersEnabled() {
        return Boolean.parseBoolean(System.getProperty(
                "cleanRoomOrders",
                System.getenv().getOrDefault("CLEAN_ROOM_ORDERS", "false")));
    }

    private static boolean loadTestAdminEnabled() {
        return Boolean.parseBoolean(System.getProperty(
                "loadTestAdminEnabled",
                System.getenv().getOrDefault("ENABLE_LOAD_TEST_ADMIN", "false")));
    }

    private static QueuedThreadPool jettyThreadPool() {
        int maxThreads = intSetting("JETTY_MAX_THREADS", "jetty.maxThreads", 512);
        int minThreads = intSetting("JETTY_MIN_THREADS", "jetty.minThreads", 64);
        int idleTimeoutMs = intSetting("JETTY_IDLE_TIMEOUT_MS", "jetty.idleTimeoutMs", 60_000);
        int reservedThreads = intSetting("JETTY_RESERVED_THREADS", "jetty.reservedThreads", 32);
        int lowThreadsThreshold = intSetting("JETTY_LOW_THREADS_THRESHOLD", "jetty.lowThreadsThreshold", 64);

        QueuedThreadPool threadPool = new QueuedThreadPool(maxThreads, minThreads, idleTimeoutMs);
        threadPool.setName("JettyServerThreadPool");
        threadPool.setReservedThreads(reservedThreads);
        threadPool.setLowThreadsThreshold(lowThreadsThreshold);
        return threadPool;
    }

    private static int intSetting(String envName, String propertyName, int defaultValue) {
        String configured = System.getProperty(propertyName, System.getenv(envName));
        if (configured == null || configured.isBlank()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(configured.trim());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private static void streamL2(Context ctx, MarketDataEngine marketDataEngine) throws IOException {
        String symbol = normalizeSymbol(ctx.pathParam("symbol"));
        stream(ctx, "snapshot", () -> l2Payload(marketDataEngine, symbol), MARKET_STREAM_INTERVAL_MS);
    }

    private static void streamTrades(Context ctx, MarketDataEngine marketDataEngine) throws IOException {
        String symbol = normalizeSymbol(ctx.pathParam("symbol"));
        stream(ctx, "trades", () -> tradesPayload(marketDataEngine, symbol), MARKET_STREAM_INTERVAL_MS);
    }

    private static void streamAccount(Context ctx, ExchangeRuntime runtime) throws IOException {
        String clientId = ctx.pathParam("clientId").trim();
        stream(ctx, "account", () -> accountSnapshot(runtime, clientId), ACCOUNT_STREAM_INTERVAL_MS);
    }

    private static void streamDiagnostics(Context ctx, ExchangeRuntime runtime) throws IOException {
        stream(ctx, "metrics", () -> diagnostics(runtime), METRICS_STREAM_INTERVAL_MS);
    }

    private static void stream(Context ctx, String eventName, Supplier<Object> payloadSupplier, int intervalMs)
            throws IOException {
        ctx.res().setContentType("text/event-stream");
        ctx.res().setCharacterEncoding("UTF-8");
        ctx.res().setHeader("Cache-Control", "no-cache");
        ctx.res().setHeader("Connection", "keep-alive");
        ctx.res().setHeader("X-Accel-Buffering", "no");
        ctx.res().setBufferSize(1);
        PrintWriter writer = ctx.res().getWriter();
        while (!Thread.currentThread().isInterrupted()) {
            try {
                writer.write("event: " + eventName + "\n");
                writer.write("data: " + GSON.toJson(payloadSupplier.get()) + "\n\n");
                writer.flush();
                ctx.res().flushBuffer();
                Thread.sleep(intervalMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (RuntimeException e) {
                break;
            }
            if (writer.checkError()) {
                break;
            }
        }
    }

    private static void json(io.javalin.http.Context ctx, Object payload) {
        ctx.contentType("application/json").result(GSON.toJson(payload));
    }

    private static void submitOrder(Context ctx, ExchangeRuntime runtime, OrderRequest request) {
        try {
            validateOrderRequest(request);
            List<ExchangeEvent> events = runtime.gateway().submitNewOrderAsync(
                    request.orderId() == null || request.orderId().isBlank() ? "WEB-" + UUID.randomUUID() : request.orderId(),
                    request.clientId(),
                    request.symbol(),
                    request.side(),
                    request.orderType(),
                    request.price(),
                    request.quantity(),
                    SelfTradePreventionMode.CANCEL_NEWEST,
                    System.nanoTime());
            if (events.isEmpty()) {
                ctx.status(202)
                        .contentType("application/json")
                        .result(ACCEPTED_ORDER_RESPONSE_JSON);
                return;
            }
            boolean accepted = true;
            ArrayList<EventDto> eventDtos = new ArrayList<>(events.size());
            for (ExchangeEvent event : events) {
                if (event instanceof OrderRejected) {
                    accepted = false;
                }
                eventDtos.add(eventDto(event));
            }
            String message = accepted ? "Order accepted by gateway" : rejectedMessage(runtime, request, events);
            ctx.status(accepted ? 202 : 400);
            json(ctx, new OrderResponse(accepted, message, eventDtos));
        } catch (Exception e) {
            ctx.status(400);
            json(ctx, new OrderResponse(false, validationMessage(e), List.of()));
        }
    }

    private static OrderRequest parseOrderRequest(Reader source) throws IOException {
        String orderId = null;
        String clientId = null;
        String symbol = null;
        Side side = null;
        OrderType orderType = null;
        Price price = null;
        int quantity = 0;

        JsonReader reader = new JsonReader(source);
        reader.beginObject();
        while (reader.hasNext()) {
            String name = reader.nextName();
            switch (name) {
                case "orderId" -> orderId = nextOptionalTrimmedString(reader);
                case "clientId" -> clientId = nextRequiredTrimmedString(reader, "clientId is required");
                case "symbol" -> symbol = normalizeSymbol(nextRequiredTrimmedString(reader, "symbol is required"));
                case "side" -> side = parseSide(nextRequiredTrimmedString(reader, "side is required"));
                case "orderType" -> orderType = parseOrderType(nextRequiredTrimmedString(reader, "orderType is required"));
                case "price" -> price = nextOptionalPrice(reader);
                case "quantity" -> quantity = nextPositiveQuantity(reader);
                default -> reader.skipValue();
            }
        }
        reader.endObject();
        return new OrderRequest(orderId, clientId, symbol, side, orderType, price, quantity);
    }

    private static String nextOptionalTrimmedString(JsonReader reader) throws IOException {
        if (reader.peek() == JsonToken.NULL) {
            reader.nextNull();
            return null;
        }
        String value = reader.nextString();
        if (value == null) {
            return null;
        }
        value = value.trim();
        return value.isEmpty() ? null : value;
    }

    private static String nextRequiredTrimmedString(JsonReader reader, String message) throws IOException {
        String value = nextOptionalTrimmedString(reader);
        if (value == null) {
            throw new IllegalArgumentException(message);
        }
        return value;
    }

    private static int nextPositiveQuantity(JsonReader reader) throws IOException {
        int quantity;
        if (reader.peek() == JsonToken.STRING) {
            String value = reader.nextString().trim();
            quantity = Integer.parseInt(value);
        } else {
            quantity = reader.nextInt();
        }
        if (quantity <= 0) {
            throw new IllegalArgumentException("quantity must be positive");
        }
        return quantity;
    }

    private static Price nextOptionalPrice(JsonReader reader) throws IOException {
        if (reader.peek() == JsonToken.NULL) {
            reader.nextNull();
            return null;
        }
        String rawPrice = reader.nextString();
        try {
            return PriceFactory.makePrice(parsePriceCents(rawPrice));
        } catch (Exception e) {
            throw new IllegalArgumentException(e.getMessage() == null ? "Invalid price value: " + rawPrice : e.getMessage(), e);
        }
    }

    private static long parsePriceCents(String rawPrice) {
        if (rawPrice == null) {
            throw new IllegalArgumentException("price is required for non-market orders");
        }
        String value = rawPrice.trim();
        if (value.isEmpty()) {
            throw new IllegalArgumentException("price is required for non-market orders");
        }

        long dollars = 0;
        int cents = 0;
        int decimalPlaces = 0;
        boolean afterDecimal = false;
        boolean sawDigit = false;
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            if (ch == '$' || ch == ',') {
                continue;
            }
            if (ch == '.') {
                if (afterDecimal) {
                    throw new IllegalArgumentException("Invalid price value: " + rawPrice);
                }
                afterDecimal = true;
                continue;
            }
            if (ch < '0' || ch > '9') {
                throw new IllegalArgumentException("Invalid price value: " + rawPrice);
            }
            sawDigit = true;
            int digit = ch - '0';
            if (afterDecimal) {
                if (decimalPlaces >= 2) {
                    throw new IllegalArgumentException("Invalid price value: " + rawPrice);
                }
                cents = cents * 10 + digit;
                decimalPlaces++;
            } else {
                dollars = Math.addExact(Math.multiplyExact(dollars, 10), digit);
            }
        }
        if (!sawDigit) {
            throw new IllegalArgumentException("Invalid price value: " + rawPrice);
        }
        if (decimalPlaces == 1) {
            cents *= 10;
        }
        return Math.addExact(Math.multiplyExact(dollars, 100), cents);
    }

    private static Side parseSide(String value) {
        if ("BUY".equalsIgnoreCase(value)) {
            return Side.BUY;
        }
        if ("SELL".equalsIgnoreCase(value)) {
            return Side.SELL;
        }
        throw new IllegalArgumentException("side is required");
    }

    private static OrderType parseOrderType(String value) {
        if ("LIMIT".equalsIgnoreCase(value)) {
            return OrderType.LIMIT;
        }
        if ("MARKET".equalsIgnoreCase(value)) {
            return OrderType.MARKET;
        }
        if ("IOC".equalsIgnoreCase(value)) {
            return OrderType.IOC;
        }
        if ("FOK".equalsIgnoreCase(value)) {
            return OrderType.FOK;
        }
        if ("STOP".equalsIgnoreCase(value)) {
            return OrderType.STOP;
        }
        if ("STOP_LIMIT".equalsIgnoreCase(value) || "STOP-LIMIT".equalsIgnoreCase(value)) {
            return OrderType.STOP_LIMIT;
        }
        throw new IllegalArgumentException("orderType is required");
    }

    private static void validateOrderRequest(OrderRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("Missing order payload");
        }
        if (request.clientId() == null) {
            throw new IllegalArgumentException("clientId is required");
        }
        if (request.symbol() == null) {
            throw new IllegalArgumentException("symbol is required");
        }
        if (request.side() == null) {
            throw new IllegalArgumentException("side is required");
        }
        if (request.orderType() == null) {
            throw new IllegalArgumentException("orderType is required");
        }
        if (request.quantity() <= 0) {
            throw new IllegalArgumentException("quantity must be positive");
        }
        if (request.orderType() != OrderType.MARKET && request.price() == null) {
            throw new IllegalArgumentException("price is required for non-market orders");
        }
        if (request.orderType() == OrderType.STOP || request.orderType() == OrderType.STOP_LIMIT) {
            throw new IllegalArgumentException("stop orders are not enabled in the web gateway");
        }
    }

    private static String rejectedMessage(ExchangeRuntime runtime, OrderRequest request, List<ExchangeEvent> events) {
        return events.stream()
                .filter(OrderRejected.class::isInstance)
                .map(OrderRejected.class::cast)
                .findFirst()
                .map(rejected -> userFacingRejectMessage(runtime, request, rejected))
                .orElse("Order rejected");
    }

    private static String userFacingRejectMessage(
            ExchangeRuntime runtime,
            OrderRequest request,
            OrderRejected rejected) {
        if (rejected.reason() == RejectReason.RISK_POSITION_LIMIT
                && rejected.message().toLowerCase(Locale.ROOT).contains("insufficient position")
                && request != null
                && request.side() != null
                && request.side() == Side.SELL) {
            String clientId = safeClientId(request);
            String symbol = request.symbol();
            ClientAccount account = runtime.riskEngine().account(clientId);
            long totalPosition = account.position(symbol);
            long reservedPosition = account.reservedPosition(symbol);
            long availableToSell = Math.max(0, totalPosition - reservedPosition);
            return "Sell order rejected. You own " + totalPosition + " " + symbol + " shares, but "
                    + reservedPosition + " are already reserved for open sell orders, leaving " + availableToSell
                    + " available to sell. Cancel an open sell order or reduce the quantity.";
        }

        return switch (rejected.reason()) {
            case RISK_BUYING_POWER ->
                    "Order rejected. Available cash is insufficient for the required order reserve.";
            case RISK_NOTIONAL_LIMIT ->
                    "Order rejected. The order value exceeds the account notional limit.";
            case RISK_POSITION_LIMIT ->
                    "Order rejected. This trade would exceed the account position limit.";
            case RISK_KILL_SWITCH ->
                    "Order rejected. Trading is currently disabled for this account or venue.";
            case WOULD_SELF_TRADE ->
                    "Order rejected. This order would trade against another open order from the same client.";
            case INVALID_PRICE ->
                    "Order rejected. Enter a valid price using the required tick size.";
            case INVALID_QUANTITY ->
                    "Order rejected. Enter a positive whole-share quantity.";
            case INVALID_SYMBOL ->
                    "Order rejected. Select a supported trading symbol.";
            default ->
                    "Order rejected. Please review the order details and try again.";
        };
    }

    private static String validationMessage(Exception e) {
        String message = e.getMessage();
        if (message == null || message.isBlank()) {
            return "Order could not be submitted. Please review the order details and try again.";
        }
        return switch (message) {
            case "clientId is required" -> "Enter a client ID before submitting the order.";
            case "symbol is required" -> "Select a trading symbol before submitting the order.";
            case "side is required" -> "Choose Buy or Sell before submitting the order.";
            case "orderType is required" -> "Choose an order type before submitting the order.";
            case "quantity must be positive" -> "Quantity must be a positive whole number.";
            case "price is required for non-market orders" -> "Enter a limit price before submitting this order.";
            case "stop orders are not enabled in the web gateway" ->
                    "Stop orders are not enabled in the web gateway yet.";
            default -> message;
        };
    }

    private static String safeClientId(OrderRequest request) {
        return request.clientId() == null || request.clientId().isBlank() ? "WEB_TRADER" : request.clientId();
    }

    private static LoadTestAccountsResponse seedLoadTestAccounts(
            ExchangeRuntime runtime,
            LoadTestAccountsRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("Missing load-test account payload");
        }
        String clientPrefix = request.clientPrefix() == null || request.clientPrefix().isBlank()
                ? "K6_TRADER"
                : request.clientPrefix().trim();
        int clientCount = Math.max(1, request.clientCount());
        long cashCents = Math.max(0L, request.cashCents());
        long positionShares = Math.max(0L, request.positionShares());
        long requestedMaxPosition = request.maxPosition() <= 0 ? 1_000_000L : request.maxPosition();
        int maxPosition = (int) Math.min(Integer.MAX_VALUE,
                Math.max(requestedMaxPosition, Math.min(Integer.MAX_VALUE, positionShares + 1_000_000L)));
        boolean shortSellingEnabled = request.shortSellingEnabled();
        List<String> symbols = request.symbols() == null || request.symbols().isEmpty()
                ? symbols()
                : request.symbols().stream().map(WebDashboard::normalizeSymbol).toList();

        for (int i = 1; i <= clientCount; i++) {
            String clientId = clientPrefix + "_" + i;
            runtime.riskEngine().setProfile(clientId, new RiskProfile(
                    Math.max(cashCents, 10_000_000_000L),
                    maxPosition,
                    cashCents,
                    false));
            runtime.riskEngine().setAvailableCash(clientId, cashCents);
            runtime.riskEngine().setShortSellingEnabled(clientId, shortSellingEnabled);
            if (positionShares > 0) {
                for (String symbol : symbols) {
                    runtime.riskEngine().setPosition(clientId, symbol, positionShares);
                }
            }
        }
        return new LoadTestAccountsResponse(clientPrefix, clientCount, cashCents, positionShares,
                maxPosition, shortSellingEnabled, symbols);
    }

    private static Map<String, Object> statusPayload(ExchangeRuntime runtime) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("running", true);
        payload.put("message", "ECN runtime operational");
        payload.put("timestamp", Instant.now().toString());
        payload.put("symbols", symbols());
        payload.put("journalSize", runtime.journal().size());
        payload.put("journalTotalAppended", runtime.journal().totalAppended());
        payload.put("eventCount", runtime.dispatcher().events().size());
        return payload;
    }

    private static List<String> symbols() {
        try {
            return ProductManager.getInstance().getProductList().stream().sorted().toList();
        } catch (Exception e) {
            return List.of();
        }
    }

    private static List<MarketCardDto> allMarketSnapshots(MarketDataEngine marketDataEngine) {
        return symbols().stream()
                .map(symbol -> {
                    L2Snapshot snapshot = marketDataEngine.l2Snapshot(symbol, 1);
                    L2Level bid = snapshot.bids().isEmpty() ? null : snapshot.bids().get(0);
                    L2Level ask = snapshot.asks().isEmpty() ? null : snapshot.asks().get(0);
                    TradeRecord lastTrade = latestTrade(marketDataEngine, symbol);
                    return new MarketCardDto(symbol, priceString(bid), quantityString(bid), priceString(ask),
                            quantityString(ask), lastTrade == null ? "N/A" : lastTrade.price().toString());
                })
                .toList();
    }

    private static L2Payload l2Payload(MarketDataEngine marketDataEngine, String symbol) {
        L2Snapshot snapshot = marketDataEngine.l2Snapshot(symbol, L2_DEPTH);
        return new L2Payload(symbol, snapshot.asOf().toString(), levels(snapshot.bids()), levels(snapshot.asks()));
    }

    private static List<LevelDto> levels(List<L2Level> levels) {
        long cumulative = 0;
        ArrayList<LevelDto> response = new ArrayList<>(levels.size());
        for (L2Level level : levels) {
            cumulative += level.quantity();
            response.add(new LevelDto(level.price().toString(), level.price().getCents(), level.quantity(), cumulative));
        }
        return response;
    }

    private static TradesPayload tradesPayload(MarketDataEngine marketDataEngine, String symbol) {
        List<TradeDto> trades = marketDataEngine.tradeTape().stream()
                .filter(trade -> trade.symbol().equals(symbol))
                .skip(Math.max(0, marketDataEngine.tradeTape().stream().filter(trade -> trade.symbol().equals(symbol)).count() - TAPE_LIMIT))
                .map(WebDashboard::tradeDto)
                .toList();
        return new TradesPayload(symbol, trades);
    }

    private static TradeRecord latestTrade(MarketDataEngine marketDataEngine, String symbol) {
        List<TradeRecord> tape = marketDataEngine.tradeTape();
        for (int i = tape.size() - 1; i >= 0; i--) {
            TradeRecord trade = tape.get(i);
            if (trade.symbol().equals(symbol)) {
                return trade;
            }
        }
        return null;
    }

    private static AccountSnapshot accountSnapshot(ExchangeRuntime runtime, String clientId) {
        ClientAccount account = runtime.riskEngine().account(clientId);
        ClientAccount.CashSnapshot cash = account.cashSnapshot();
        Map<String, Long> positions = new TreeOrderedMap<>(account.positions());
        Map<String, Long> reservedPositions = new TreeOrderedMap<>(account.reservedPositions());
        HashMap<String, Long> availablePositions = new HashMap<>();
        positions.forEach((symbol, quantity) ->
                availablePositions.put(symbol, Math.max(0, quantity - reservedPositions.getOrDefault(symbol, 0L))));
        return new AccountSnapshot(account.clientId(), money(cash.availableCashCents()), cash.availableCashCents(),
                money(cash.reservedCashCents()), cash.reservedCashCents(), positions, reservedPositions,
                new TreeOrderedMap<>(availablePositions), workingOrders(runtime, clientId));
    }

    private static List<WorkingOrderDto> workingOrders(ExchangeRuntime runtime, String clientId) {
        return runtime.riskEngine().restingOrdersFor(clientId).stream()
                .limit(100)
                .map(WebDashboard::workingOrderDto)
                .toList();
    }

    private static WorkingOrderDto workingOrderDto(RestingOrderSnapshot order) {
        return new WorkingOrderDto(order.orderId(), order.symbol(), order.side().name(), order.orderType().name(),
                money(order.priceCents()), order.priceCents(), order.quantity(), Math.toIntExact(order.leavesQty()),
                order.cumQty(), order.sequenceNumber());
    }

    private static DiagnosticsPayload diagnostics(ExchangeRuntime runtime) {
        return new DiagnosticsPayload(
                Instant.now().toString(),
                telemetry.throughputPerSecond(),
                telemetry.latencyMicros(),
                runtime.gateway().shardStatuses().stream()
                        .map(status -> new ShardDto(status.symbol(), status.running(), status.queuedCommands(),
                                status.completedCommands()))
                        .toList(),
                candles(runtime.marketDataEngine().closedCandles()));
    }

    private static List<CandleDto> candles(List<OhlcvCandle> candles) {
        return candles.stream()
                .map(candle -> new CandleDto(candle.symbol(), candle.windowStart().toString(), candle.windowEnd().toString(),
                        candle.open().toString(), candle.high().toString(), candle.low().toString(),
                        candle.close().toString(), candle.volume()))
                .toList();
    }

    private static EventDto eventDto(ExchangeEvent event) {
        String type = event.getClass().getSimpleName();
        String message = type;
        if (event instanceof OrderRejected rejected) {
            message = rejected.reason() + ": " + rejected.message();
        } else if (event instanceof OrderCancelled cancelled) {
            message = "Cancelled " + cancelled.cancelledQty() + " - " + cancelled.reason();
        } else if (event instanceof OrderExecuted executed) {
            message = "Filled " + executed.fillQty() + " @ " + executed.fillPrice();
        }
        return new EventDto(type, event.sequenceNumber(), event.orderId(), event.clientId(), event.symbol(),
                event.eventTimestamp().toString(), message);
    }

    private static TradeDto tradeDto(TradeRecord trade) {
        return new TradeDto(trade.sequenceNumber(), trade.symbol(), trade.price().toString(), trade.price().getCents(),
                trade.quantity(), trade.timestamp().toString(), trade.takerSide().name());
    }

    private static String normalizeSymbol(String symbol) {
        return symbol.trim().toUpperCase(Locale.ROOT);
    }

    private static String priceString(L2Level level) {
        return level == null ? "N/A" : level.price().toString();
    }

    private static String quantityString(L2Level level) {
        return level == null ? "0" : String.valueOf(level.quantity());
    }

    private static String money(long cents) {
        long abs = Math.abs(cents);
        return String.format("%s$%,d.%02d", cents < 0 ? "-" : "", abs / 100, abs % 100);
    }

    public static void stop() {
        if (app != null) {
            app.stop();
            app = null;
        }
        if (telemetry != null) {
            ExchangeRuntime.getInstance().dispatcher().removeRingBufferListener(telemetry);
            telemetry = null;
        }
    }

    private static final class DashboardTelemetry implements RingBufferEventListener {
        private final ConcurrentHashMap<String, AtomicLong> totalOrdersBySymbol = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<String, AtomicLong> lastOrdersBySymbol = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<String, Counter> acceptedCountersBySymbol = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<String, Counter> rejectedCountersBySymbol = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<String, Timer> latencyTimersBySymbol = new ConcurrentHashMap<>();
        private final ArrayDeque<Long> latencyNanos = new ArrayDeque<>(LATENCY_LIMIT);

        @Override
        public synchronized void onRingBufferEvent(RingBufferEvent event, long sequence, boolean endOfBatch) {
            String symbol = event.getSymbol();
            if (event.getEventType() == RingBufferEvent.EventType.REJECTED) {
                Counter counter = meterCounter(rejectedCountersBySymbol, "orders_rejected_total", symbol);
                if (counter != null) {
                    counter.increment();
                }
                return;
            }

            if (event.getEventType() == RingBufferEvent.EventType.ACCEPTED) {
                Counter counter = meterCounter(acceptedCountersBySymbol, "orders_accepted_total", symbol);
                if (counter != null) {
                    counter.increment();
                }
                totalOrdersBySymbol.computeIfAbsent(symbol, ignored -> new AtomicLong()).incrementAndGet();
            }

            if (event.getEventType() == RingBufferEvent.EventType.EXECUTED
                    && event.getEventEmittedNanos() >= event.getEngineInNanos()) {
                long lat = event.getEventEmittedNanos() - event.getEngineInNanos();
                latencyNanos.addLast(lat);
                Timer timer = meterTimer(symbol);
                if (timer != null) {
                    timer.record(lat, java.util.concurrent.TimeUnit.NANOSECONDS);
                }
                while (latencyNanos.size() > LATENCY_LIMIT) {
                    latencyNanos.removeFirst();
                }
            }
        }

        private Counter meterCounter(ConcurrentHashMap<String, Counter> counters, String name, String symbol) {
            PrometheusMeterRegistry registry = prometheusRegistry;
            if (registry == null || symbol == null) {
                return null;
            }
            Counter existing = counters.get(symbol);
            if (existing != null) {
                return existing;
            }
            Counter created = registry.counter(name, "symbol", symbol);
            Counter raced = counters.putIfAbsent(symbol, created);
            return raced == null ? created : raced;
        }

        private Timer meterTimer(String symbol) {
            PrometheusMeterRegistry registry = prometheusRegistry;
            if (registry == null || symbol == null) {
                return null;
            }
            Timer existing = latencyTimersBySymbol.get(symbol);
            if (existing != null) {
                return existing;
            }
            Timer created = registry.timer("order_latency_seconds", "symbol", symbol);
            Timer raced = latencyTimersBySymbol.putIfAbsent(symbol, created);
            return raced == null ? created : raced;
        }

        private synchronized Map<String, Long> throughputPerSecond() {
            HashMap<String, Long> result = new HashMap<>();
            for (Map.Entry<String, AtomicLong> entry : totalOrdersBySymbol.entrySet()) {
                long current = entry.getValue().get();
                AtomicLong previous = lastOrdersBySymbol.computeIfAbsent(entry.getKey(), ignored -> new AtomicLong(current));
                result.put(entry.getKey(), Math.max(0, current - previous.getAndSet(current)));
            }
            return new TreeOrderedMap<>(result);
        }

        private synchronized List<Long> latencyMicros() {
            return latencyNanos.stream().map(nanos -> nanos / 1_000L).toList();
        }
    }

    private static final class TreeOrderedMap<V> extends LinkedHashMap<String, V> {
        private TreeOrderedMap(Map<String, V> source) {
            source.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .forEach(entry -> put(entry.getKey(), entry.getValue()));
        }
    }

    private record OrderRequest(String orderId, String clientId, String symbol, Side side, OrderType orderType,
            Price price, int quantity) {
    }

    private record LoadTestAccountsRequest(String clientPrefix, int clientCount, long cashCents,
            long positionShares, int maxPosition, boolean shortSellingEnabled, List<String> symbols) {
    }

    private record LoadTestAccountsResponse(String clientPrefix, int clientCount, long cashCents,
            long positionShares, int maxPosition, boolean shortSellingEnabled, List<String> symbols) {
    }

    private record OrderResponse(boolean accepted, String message, List<EventDto> events) {
    }

    private record EventDto(String type, long sequenceNumber, String orderId, String clientId, String symbol,
            String timestamp, String message) {
    }

    private record MarketCardDto(String symbol, String bid, String bidVol, String ask, String askVol, String last) {
    }

    private record L2Payload(String symbol, String asOf, List<LevelDto> bids, List<LevelDto> asks) {
    }

    private record LevelDto(String price, long priceCents, long quantity, long cumulativeQuantity) {
    }

    private record TradesPayload(String symbol, List<TradeDto> trades) {
    }

    private record TradeDto(long sequenceNumber, String symbol, String price, long priceCents, int quantity,
            String timestamp, String takerSide) {
    }

    private record AccountSnapshot(String clientId, String availableCash, long availableCashCents, String reservedCash,
            long reservedCashCents, Map<String, Long> positions, Map<String, Long> reservedPositions,
            Map<String, Long> availablePositions, List<WorkingOrderDto> workingOrders) {
    }

    private record WorkingOrderDto(String orderId, String symbol, String side, String orderType, String price,
            long priceCents, int quantity, int leavesQty, int cumQty, long sequenceNumber) {
    }


    private record DiagnosticsPayload(String timestamp, Map<String, Long> ordersPerSecondBySymbol,
            List<Long> latencyMicros, List<ShardDto> shards, List<CandleDto> candles) {
    }

    private record ShardDto(String symbol, boolean running, int queuedCommands, long completedCommands) {
    }

    private record CandleDto(String symbol, String windowStart, String windowEnd, String open, String high, String low,
            String close, long volume) {
    }
}
