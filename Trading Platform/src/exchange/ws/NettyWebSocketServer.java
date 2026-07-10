package exchange.ws;

import Price.Price;
import Price.PriceFactory;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import exchange.core.ExchangeRuntime;
import exchange.dispatch.EventDispatcher;
import exchange.dispatch.EventListener;
import exchange.dispatch.InMemoryEventDispatcher;
import exchange.dispatch.RingBufferEvent;
import exchange.dispatch.RingBufferEventListener;
import exchange.gateway.OrderGateway;
import exchange.marketdata.L2Level;
import exchange.marketdata.L2Snapshot;
import exchange.marketdata.MarketDataEngine;
import exchange.marketdata.MarketDataListener;
import exchange.marketdata.TradeRecord;
import exchange.model.ExchangeEvent;
import exchange.model.OrderRejected;
import exchange.model.OrderType;
import exchange.model.RejectReason;
import exchange.model.SelfTradePreventionMode;
import exchange.model.Side;
import exchange.telemetry.LatencyTelemetry;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollChannelOption;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PingWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PongWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketServerHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketServerHandshakerFactory;
import io.netty.util.AttributeKey;
import io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public final class NettyWebSocketServer implements AutoCloseable, EventListener, RingBufferEventListener,
        MarketDataListener {
    private static final Logger logger = LoggerFactory.getLogger(NettyWebSocketServer.class);
    private static final String MARKET_DATA_PATH = "/ws/market-data";
    private static final String ORDERS_PATH = "/ws/orders";
    private static final int DEFAULT_L2_DEPTH = 10;
    private static final int DEFAULT_TRADE_TAPE_LIMIT = 100;
    /** Linux busy-poll window in microseconds; reduces wake latency for ingress frames. */
    private static final int SO_BUSY_POLL_MICROS = 50;
    private static final WriteBufferWaterMark WRITE_BUFFER_WATERMARK =
            new WriteBufferWaterMark(
                    intSetting("WS_WRITE_BUFFER_LOW_BYTES", "ws.writeBufferLowBytes", 1 << 20),
                    intSetting("WS_WRITE_BUFFER_HIGH_BYTES", "ws.writeBufferHighBytes", 8 << 20));

    private static final AttributeKey<ChannelRole> ROLE_KEY = AttributeKey.valueOf("exchange.ws.role");
    private static final AttributeKey<String> SYMBOL_KEY = AttributeKey.valueOf("exchange.ws.symbol");
    private static final AttributeKey<String> CLIENT_ID_KEY = AttributeKey.valueOf("exchange.ws.clientId");
    private static final AttributeKey<WebSocketServerHandshaker> HANDSHAKER_KEY = AttributeKey.valueOf("exchange.ws.handshaker");
    private static final AttributeKey<AtomicBoolean> FLUSH_SCHEDULED_KEY =
            AttributeKey.valueOf("exchange.ws.flushScheduled");
    private static final AttributeKey<AtomicBoolean> SLOW_CONSUMER_CLOSING_KEY =
            AttributeKey.valueOf("exchange.ws.slowConsumerClosing");
    private static final ThreadLocal<RingBufferEvent> IMMEDIATE_REPORT =
            ThreadLocal.withInitial(RingBufferEvent::new);

    private final Gson gson = new Gson();
    private final OrderGateway gateway;
    private final EventDispatcher dispatcher;
    private final MarketDataEngine marketDataEngine;
    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;
    private final ServerBootstrap bootstrap;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final Set<Channel> openChannels = ConcurrentHashMap.newKeySet();
    private final ConcurrentHashMap<String, Set<Channel>> marketDataSubscribersBySymbol = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Set<Channel>> orderChannelsByClientId = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Channel> primaryOrderChannelByClientId = new ConcurrentHashMap<>();
    private final AtomicBoolean usingRingBufferListener = new AtomicBoolean(false);

    private volatile Channel serverChannel;
    private volatile int port;

    public NettyWebSocketServer(ExchangeRuntime runtime, int port) {
        this(runtime.gateway(), runtime.dispatcher(), runtime.marketDataEngine(), port);
    }

    public NettyWebSocketServer(OrderGateway gateway, EventDispatcher dispatcher, MarketDataEngine marketDataEngine, int port) {
        this.gateway = gateway;
        this.dispatcher = dispatcher;
        this.marketDataEngine = marketDataEngine;
        this.port = port;
        ThreadFactory bossFactory = namedThreadFactory("netty-ws-boss");
        ThreadFactory workerFactory = namedThreadFactory("netty-ws-worker");
        boolean useEpoll = Epoll.isAvailable();
        if (useEpoll) {
            this.bossGroup = new EpollEventLoopGroup(1, bossFactory);
            this.workerGroup = new EpollEventLoopGroup(Math.max(2, Runtime.getRuntime().availableProcessors() / 2),
                    workerFactory);
        } else {
            this.bossGroup = new NioEventLoopGroup(1, bossFactory);
            this.workerGroup = new NioEventLoopGroup(Math.max(2, Runtime.getRuntime().availableProcessors() / 2),
                    workerFactory);
        }
        Class<? extends ServerSocketChannel> serverChannelClass = useEpoll
                ? EpollServerSocketChannel.class
                : NioServerSocketChannel.class;
        this.bootstrap = new ServerBootstrap()
                .group(bossGroup, workerGroup)
                .channel(serverChannelClass)
                .option(ChannelOption.SO_REUSEADDR, true)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.WRITE_BUFFER_WATER_MARK, WRITE_BUFFER_WATERMARK)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel channel) {
                        applyLowLatencySocketOptions(channel, useEpoll);
                        ChannelPipeline pipeline = channel.pipeline();
                        pipeline.addLast(new HttpServerCodec());
                        pipeline.addLast(new HttpObjectAggregator(65_536));
                        pipeline.addLast(new WebSocketChannelHandler());
                    }
                });
        if (useEpoll) {
            bootstrap.childOption(EpollChannelOption.SO_BUSY_POLL, SO_BUSY_POLL_MICROS);
            logger.info("Netty WebSocket transport: epoll (SO_BUSY_POLL={}us, TCP_NODELAY=true)",
                    SO_BUSY_POLL_MICROS);
        } else {
            logger.info("Netty WebSocket transport: nio (TCP_NODELAY=true; SO_BUSY_POLL requires Linux epoll)");
        }
    }

    private static void applyLowLatencySocketOptions(SocketChannel channel, boolean useEpoll) {
        channel.config().setOption(ChannelOption.TCP_NODELAY, true);
        if (useEpoll) {
            channel.config().setOption(EpollChannelOption.SO_BUSY_POLL, SO_BUSY_POLL_MICROS);
        }
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

    public synchronized void start() {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        ChannelFuture bindFuture = bootstrap.bind(port).syncUninterruptibly();
        serverChannel = bindFuture.channel();
        port = ((InetSocketAddress) serverChannel.localAddress()).getPort();
        if (dispatcher instanceof InMemoryEventDispatcher inMemoryDispatcher) {
            inMemoryDispatcher.addRingBufferListener(this);
            usingRingBufferListener.set(true);
        } else {
            dispatcher.addListener(this);
        }
        marketDataEngine.addListener(this);
        logger.info("Netty WebSocket API started on ws://localhost:{}", port);
    }

    public int port() {
        return port;
    }

    public int marketDataSubscriberCount(String symbol) {
        Set<Channel> subscribers = marketDataSubscribersBySymbol.get(symbol);
        return subscribers == null ? 0 : subscribers.size();
    }

    @Override
    public void onEvents(List<ExchangeEvent> events) {
        // Immutable ExchangeEvent fanout is intentionally disabled for WebSocket
        // execution reports. The ring-buffer listener below is the sole order-report
        // publication path so ACKs do not pass through the legacy Gson/Map envelope.
    }

    @Override
    public void onRingBufferEvent(RingBufferEvent event, long sequence, boolean endOfBatch) {
        if (!started.get()) {
            return;
        }
        broadcastExecutionReport(event, endOfBatch);
    }

    @Override
    public boolean hasSubscribers(String symbol) {
        Set<Channel> subscribers = marketDataSubscribersBySymbol.get(symbol);
        return subscribers != null && !subscribers.isEmpty();
    }

    @Override
    public void onL2Snapshot(L2Snapshot snapshot) {
        if (!started.get()) {
            return;
        }
        Set<Channel> subscribers = marketDataSubscribersBySymbol.get(snapshot.symbol());
        if (subscribers == null || subscribers.isEmpty()) {
            return;
        }
        MarketDataEnvelope payload = new MarketDataEnvelope("l2", snapshot.symbol(), snapshot.asOf().toString(),
                levels(snapshot.bids()), levels(snapshot.asks()), List.of());
        for (Channel channel : subscribers) {
            writeJson(channel, payload);
        }
    }

    @Override
    public void onTradeTape(String symbol, List<TradeRecord> trades) {
        if (!started.get()) {
            return;
        }
        Set<Channel> subscribers = marketDataSubscribersBySymbol.get(symbol);
        if (subscribers == null || subscribers.isEmpty()) {
            return;
        }
        MarketDataEnvelope payload = new MarketDataEnvelope("trades", symbol, null, List.of(), List.of(),
                trades.stream().map(NettyWebSocketServer::tradeDto).toList());
        for (Channel channel : subscribers) {
            writeJson(channel, payload);
        }
    }

    @Override
    public synchronized void close() {
        if (!started.compareAndSet(true, false)) {
            return;
        }
        if (usingRingBufferListener.get() && dispatcher instanceof InMemoryEventDispatcher inMemoryDispatcher) {
            inMemoryDispatcher.removeRingBufferListener(this);
        } else {
            dispatcher.removeListener(this);
        }
        marketDataEngine.removeListener(this);
        for (Channel channel : List.copyOf(openChannels)) {
            closeChannel(channel);
        }
        Channel listeningChannel = serverChannel;
        if (listeningChannel != null) {
            listeningChannel.close().syncUninterruptibly();
        }
        workerGroup.shutdownGracefully().syncUninterruptibly();
        bossGroup.shutdownGracefully().syncUninterruptibly();
    }

    private void handleHandshake(ChannelHandlerContext context, FullHttpRequest request) {
        if (!request.decoderResult().isSuccess()) {
            sendHttpError(context, request, HttpResponseStatus.BAD_REQUEST);
            return;
        }

        URI uri = URI.create(request.uri());
        String path = uri.getPath();
        ChannelRole role = switch (path) {
            case MARKET_DATA_PATH -> ChannelRole.MARKET_DATA;
            case ORDERS_PATH -> ChannelRole.ORDERS;
            default -> null;
        };
        if (role == null) {
            sendHttpError(context, request, HttpResponseStatus.NOT_FOUND);
            return;
        }

        WebSocketServerHandshakerFactory handshakerFactory =
                new WebSocketServerHandshakerFactory(webSocketLocation(request, path), null, true);
        WebSocketServerHandshaker handshaker = handshakerFactory.newHandshaker(request);
        if (handshaker == null) {
            WebSocketServerHandshakerFactory.sendUnsupportedVersionResponse(context.channel());
            return;
        }

        Channel channel = context.channel();
        channel.attr(ROLE_KEY).set(role);
        channel.attr(HANDSHAKER_KEY).set(handshaker);
        channel.attr(FLUSH_SCHEDULED_KEY).set(new AtomicBoolean(false));
        channel.attr(SLOW_CONSUMER_CLOSING_KEY).set(new AtomicBoolean(false));
        openChannels.add(channel);
        handshaker.handshake(channel, request);
    }

    private void handleWebSocketFrame(ChannelHandlerContext context, WebSocketFrame frame) {
        if (frame instanceof CloseWebSocketFrame closeFrame) {
            WebSocketServerHandshaker handshaker = context.channel().attr(HANDSHAKER_KEY).get();
            if (handshaker != null) {
                handshaker.close(context.channel(), closeFrame.retain());
            } else {
                context.close();
            }
            return;
        }
        if (frame instanceof PingWebSocketFrame pingFrame) {
            context.channel().writeAndFlush(new PongWebSocketFrame(pingFrame.content().retain()));
            return;
        }
        if (frame instanceof BinaryWebSocketFrame binaryFrame) {
            ChannelRole role = context.channel().attr(ROLE_KEY).get();
            if (role == ChannelRole.ORDERS) {
                handleBinaryOrderCommand(context.channel(), binaryFrame.content(), System.nanoTime());
            } else {
                context.writeAndFlush(new TextWebSocketFrame(gson.toJson(new ErrorEnvelope("error",
                        "Binary frames are only supported on /ws/orders"))));
            }
            return;
        }
        if (!(frame instanceof TextWebSocketFrame textFrame)) {
            throw new IllegalStateException("Unsupported frame type: " + frame.getClass().getSimpleName());
        }

        ChannelRole role = context.channel().attr(ROLE_KEY).get();
        if (role == ChannelRole.MARKET_DATA) {
            handleMarketDataCommand(context.channel(), textFrame.text());
        } else if (role == ChannelRole.ORDERS) {
            handleOrderCommand(context.channel(), textFrame.text(), System.nanoTime());
        } else {
            context.writeAndFlush(new TextWebSocketFrame(gson.toJson(new ErrorEnvelope("error",
                    "WebSocket channel is not initialized"))));
        }
    }

    private void handleMarketDataCommand(Channel channel, String text) {
        try {
            JsonObject payload = JsonParser.parseString(text).getAsJsonObject();
            String commandType = requiredString(payload, "type").toLowerCase(Locale.ROOT);
            if ("subscribe".equals(commandType)) {
                String symbol = requiredString(payload, "symbol").trim().toUpperCase(Locale.ROOT);
                subscribeToMarketData(channel, symbol);
                writeJson(channel, new SubscriptionEnvelope("subscribed", symbol, Instant.now().toString()));
            } else if ("unsubscribe".equals(commandType)) {
                unsubscribeFromMarketData(channel);
                writeJson(channel, new SubscriptionEnvelope("unsubscribed", null, Instant.now().toString()));
            } else {
                writeJson(channel, new ErrorEnvelope("error", "Unsupported market-data command: " + commandType));
            }
        } catch (Exception e) {
            writeJson(channel, new ErrorEnvelope("error", e.getMessage()));
        }
    }

    private void subscribeToMarketData(Channel channel, String symbol) {
        String priorSymbol = channel.attr(SYMBOL_KEY).get();
        if (priorSymbol != null && !priorSymbol.equals(symbol)) {
            Set<Channel> subscribers = marketDataSubscribersBySymbol.get(priorSymbol);
            if (subscribers != null) {
                subscribers.remove(channel);
            }
        }

        channel.attr(SYMBOL_KEY).set(symbol);
        marketDataSubscribersBySymbol.computeIfAbsent(symbol, ignored -> ConcurrentHashMap.newKeySet()).add(channel);
        writeJson(channel, new MarketDataEnvelope("l2", symbol,
                marketDataEngine.l2Snapshot(symbol, DEFAULT_L2_DEPTH).asOf().toString(),
                levels(marketDataEngine.l2Snapshot(symbol, DEFAULT_L2_DEPTH).bids()),
                levels(marketDataEngine.l2Snapshot(symbol, DEFAULT_L2_DEPTH).asks()),
                List.of()));
        writeJson(channel, new MarketDataEnvelope("trades", symbol, null, List.of(), List.of(),
                marketDataEngine.tradeTape(symbol, DEFAULT_TRADE_TAPE_LIMIT).stream().map(NettyWebSocketServer::tradeDto).toList()));
    }

    private void unsubscribeFromMarketData(Channel channel) {
        String symbol = channel.attr(SYMBOL_KEY).getAndSet(null);
        if (symbol == null) {
            return;
        }
        Set<Channel> subscribers = marketDataSubscribersBySymbol.get(symbol);
        if (subscribers != null) {
            subscribers.remove(channel);
        }
    }

    private void handleOrderCommand(Channel channel, String text, long ingressTimeNs) {
        String clientId = null;
        String symbol = null;
        String orderId = null;
        try {
            if (!OrderCommandJsonScanner.isNewOrder(text)) {
                writeJson(channel, new ErrorEnvelope("error", "Unsupported order command"));
                return;
            }

            clientId = OrderCommandJsonScanner.requiredString(text, OrderCommandJsonScanner.CLIENT_ID_KEY,
                    "clientId");
            symbol = OrderCommandJsonScanner.requiredString(text, OrderCommandJsonScanner.SYMBOL_KEY,
                    "symbol");
            Side side = OrderCommandJsonScanner.side(text);
            OrderType orderType = OrderCommandJsonScanner.orderType(text);
            int quantity = OrderCommandJsonScanner.quantity(text);
            if (quantity <= 0) {
                throw new IllegalArgumentException("quantity must be positive");
            }
            Price price = null;
            if (orderType != OrderType.MARKET) {
                price = PriceFactory.makePrice(OrderCommandJsonScanner.priceCents(text));
            }
            orderId = OrderCommandJsonScanner.optionalString(text, OrderCommandJsonScanner.ORDER_ID_KEY);
            if (orderId == null) {
                orderId = "WS-" + UUID.randomUUID();
            }
            SelfTradePreventionMode stpMode = OrderCommandJsonScanner.stpMode(text);

            bindOrderChannel(clientId, channel);
            List<ExchangeEvent> immediateEvents = gateway.submitNewOrderAsync(orderId, clientId, symbol, side,
                    orderType, price, quantity, stpMode, ingressTimeNs);
            writeImmediateReports(channel, immediateEvents, ingressTimeNs);
        } catch (Exception e) {
            if (orderId != null && clientId != null && symbol != null) {
                writeImmediateReject(channel, orderId, clientId, symbol, RejectReason.INVALID_ORDER_ID,
                        e.getMessage() == null ? "Order command failed" : e.getMessage(), ingressTimeNs);
            } else {
                writeJson(channel, new ErrorEnvelope("error", e.getMessage()));
            }
        }
    }

    private void handleBinaryOrderCommand(Channel channel, ByteBuf payload, long ingressTimeNs) {
        BinaryOrderCodec.BinaryNewOrder order = null;
        try {
            order = BinaryOrderCodec.decodeNewOrder(payload);
            bindOrderChannel(order.clientId(), channel);
            List<ExchangeEvent> immediateEvents = gateway.submitNewOrderAsync(order.orderId(), order.clientId(), order.symbol(), order.side(),
                    order.orderType(), order.price(), order.quantity(), order.stpMode(), ingressTimeNs);
            writeImmediateReports(channel, immediateEvents, ingressTimeNs);
        } catch (Exception e) {
            if (order != null) {
                writeImmediateReject(channel, order.orderId(), order.clientId(), order.symbol(),
                        RejectReason.INVALID_ORDER_ID,
                        e.getMessage() == null ? "Order command failed" : e.getMessage(), ingressTimeNs);
            } else {
                writeJson(channel, new ErrorEnvelope("error", e.getMessage()));
            }
        }
    }

    private void bindOrderChannel(String clientId, Channel channel) {
        String previousClientId = channel.attr(CLIENT_ID_KEY).get();
        if (previousClientId != null && !previousClientId.equals(clientId)) {
            Set<Channel> previousChannels = orderChannelsByClientId.get(previousClientId);
            if (previousChannels != null) {
                previousChannels.remove(channel);
            }
        }
        channel.attr(CLIENT_ID_KEY).set(clientId);
        primaryOrderChannelByClientId.put(clientId, channel);
        orderChannelsByClientId.computeIfAbsent(clientId, ignored -> ConcurrentHashMap.newKeySet()).add(channel);
    }

    private void broadcastExecutionReport(RingBufferEvent event, boolean flush) {
        String clientId = event.getClientId();
        Channel primary = primaryOrderChannelByClientId.get(clientId);
        if (primary != null) {
            writeExecutionReport(primary, event, flush);
        }
        Set<Channel> subscribers = orderChannelsByClientId.get(clientId);
        if (subscribers == null || subscribers.isEmpty()) {
            return;
        }
        if (primary != null && subscribers.size() == 1) {
            return;
        }
        for (Channel channel : subscribers) {
            if (channel == primary) {
                continue;
            }
            writeExecutionReport(channel, event, flush);
        }
    }

    private void writeExecutionReport(Channel channel, RingBufferEvent event, boolean flush) {
        if (!channel.isActive()) {
            removeChannel(channel);
            return;
        }
        if (!channel.isWritable()) {
            closeSlowConsumer(channel);
            return;
        }
        long ingressTimeNs = event.getEngineInNanos();
        TextWebSocketFrame frame = new TextWebSocketFrame(ExecutionReportJsonEncoder.encode(channel, event));
        LatencyTelemetry.getInstance().recordTickToTrade(ingressTimeNs, System.nanoTime());
        if (flush) {
            channel.writeAndFlush(frame);
        } else {
            channel.write(frame);
            scheduleFlush(channel);
        }
    }

    private void writeImmediateReports(Channel channel, List<ExchangeEvent> events, long ingressTimeNs) {
        if (events.isEmpty()) {
            return;
        }
        RingBufferEvent scratch = IMMEDIATE_REPORT.get();
        int batchSize = events.size();
        for (int i = 0; i < batchSize; i++) {
            scratch.copyFrom(events.get(i), i, batchSize);
            scratch.setLatencyNanos(ingressTimeNs, System.nanoTime());
            writeExecutionReport(channel, scratch, true);
        }
        scratch.clear();
    }

    private void writeImmediateReject(Channel channel, String orderId, String clientId, String symbol,
            RejectReason reason, String message, long ingressTimeNs) {
        RingBufferEvent scratch = IMMEDIATE_REPORT.get();
        scratch.copyFrom(new OrderRejected(0L, orderId, clientId, symbol, reason, message, Instant.EPOCH), 0, 1);
        scratch.setLatencyNanos(ingressTimeNs, System.nanoTime());
        writeExecutionReport(channel, scratch, true);
        scratch.clear();
    }

    private void scheduleFlush(Channel channel) {
        AtomicBoolean scheduled = channel.attr(FLUSH_SCHEDULED_KEY).get();
        if (scheduled == null) {
            channel.flush();
            return;
        }
        if (!scheduled.compareAndSet(false, true)) {
            return;
        }
        channel.eventLoop().execute(() -> {
            scheduled.set(false);
            if (channel.isActive()) {
                channel.flush();
            }
        });
    }

    private void writeJson(Channel channel, Object payload) {
        if (!channel.isActive()) {
            removeChannel(channel);
            return;
        }
        if (!channel.isWritable()) {
            closeSlowConsumer(channel);
            return;
        }
        channel.eventLoop().execute(() -> {
            if (!channel.isActive()) {
                removeChannel(channel);
                return;
            }
            if (!channel.isWritable()) {
                closeSlowConsumer(channel);
                return;
            }
            channel.writeAndFlush(new TextWebSocketFrame(gson.toJson(payload)));
        });
    }

    private void closeSlowConsumer(Channel channel) {
        AtomicBoolean closing = channel.attr(SLOW_CONSUMER_CLOSING_KEY).get();
        if (closing != null && !closing.compareAndSet(false, true)) {
            return;
        }
        logger.warn("Closing slow WebSocket consumer {}: outbound buffer exceeded high watermark",
                channel.remoteAddress());
        closeChannel(channel);
    }

    private void closeChannel(Channel channel) {
        removeChannel(channel);
        if (channel.isOpen()) {
            channel.eventLoop().execute(() -> channel.close());
        }
    }

    private void removeChannel(Channel channel) {
        openChannels.remove(channel);
        String symbol = channel.attr(SYMBOL_KEY).get();
        if (symbol != null) {
            Set<Channel> subscribers = marketDataSubscribersBySymbol.get(symbol);
            if (subscribers != null) {
                subscribers.remove(channel);
            }
        }
        String clientId = channel.attr(CLIENT_ID_KEY).get();
        if (clientId != null) {
            Set<Channel> subscribers = orderChannelsByClientId.get(clientId);
            if (subscribers != null) {
                subscribers.remove(channel);
            }
            primaryOrderChannelByClientId.remove(clientId, channel);
        }
    }

    private static List<LevelDto> levels(List<L2Level> levels) {
        long cumulative = 0;
        ArrayList<LevelDto> response = new ArrayList<>(levels.size());
        for (L2Level level : levels) {
            cumulative += level.quantity();
            response.add(new LevelDto(formatPrice(level.price()), level.price().getCents(), level.quantity(), cumulative));
        }
        return List.copyOf(response);
    }

    private static TradeDto tradeDto(TradeRecord trade) {
        return new TradeDto(trade.sequenceNumber(), trade.symbol(), formatPrice(trade.price()), trade.price().getCents(),
                trade.quantity(), trade.timestamp().toString(), trade.takerSide().name());
    }

    private static String formatPrice(Price price) {
        if (price == null) {
            return null;
        }
        long cents = price.getCents();
        long absCents = Math.abs(cents);
        long dollars = absCents / 100;
        long fractional = absCents % 100;
        int capacity = cents < 0 ? 5 : 4;
        StringBuilder builder = new StringBuilder(capacity + digitCount(dollars));
        if (cents < 0) {
            builder.append('-');
        }
        builder.append(dollars).append('.');
        if (fractional < 10) {
            builder.append('0');
        }
        builder.append(fractional);
        return builder.toString();
    }

    private static int digitCount(long value) {
        int digits = 1;
        while (value >= 10) {
            value /= 10;
            digits++;
        }
        return digits;
    }

    private static void appendJsonString(StringBuilder builder, String value) {
        if (value == null) {
            builder.append("null");
            return;
        }
        builder.append('"');
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"' -> builder.append("\\\"");
                case '\\' -> builder.append("\\\\");
                case '\b' -> builder.append("\\b");
                case '\f' -> builder.append("\\f");
                case '\n' -> builder.append("\\n");
                case '\r' -> builder.append("\\r");
                case '\t' -> builder.append("\\t");
                default -> {
                    if (c < 0x20) {
                        builder.append("\\u");
                        String hex = Integer.toHexString(c);
                        for (int pad = hex.length(); pad < 4; pad++) {
                            builder.append('0');
                        }
                        builder.append(hex);
                    } else {
                        builder.append(c);
                    }
                }
            }
        }
        builder.append('"');
    }

    private static String webSocketLocation(FullHttpRequest request, String path) {
        String protocol = "ws";
        String host = request.headers().get(HttpHeaderNames.HOST);
        return protocol + "://" + host + path;
    }

    private static void sendHttpError(ChannelHandlerContext context, FullHttpRequest request, HttpResponseStatus status) {
        FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status,
                Unpooled.copiedBuffer(status.toString(), CharsetUtil.UTF_8));
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=UTF-8");
        HttpUtil.setContentLength(response, response.content().readableBytes());
        ChannelFuture future = context.channel().writeAndFlush(response);
        if (!HttpUtil.isKeepAlive(request) || status.code() != 200) {
            future.addListener(ChannelFutureListener.CLOSE);
        }
    }

    private static String requiredString(JsonObject payload, String key) {
        if (!payload.has(key) || payload.get(key).isJsonNull()) {
            throw new IllegalArgumentException("Missing required field: " + key);
        }
        String value = payload.get(key).getAsString();
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Missing required field: " + key);
        }
        return value;
    }

    private static ThreadFactory namedThreadFactory(String prefix) {
        AtomicInteger nextId = new AtomicInteger(1);
        return runnable -> {
            Thread thread = new Thread(runnable, prefix + "-" + nextId.getAndIncrement());
            thread.setDaemon(true);
            return thread;
        };
    }

    private final class WebSocketChannelHandler extends SimpleChannelInboundHandler<Object> {
        @Override
        public void channelInactive(ChannelHandlerContext context) {
            removeChannel(context.channel());
        }

        @Override
        public void channelWritabilityChanged(ChannelHandlerContext context) {
            Channel channel = context.channel();
            if (!channel.isWritable()) {
                closeSlowConsumer(channel);
                return;
            }
            context.fireChannelWritabilityChanged();
        }

        @Override
        protected void channelRead0(ChannelHandlerContext context, Object message) {
            if (message instanceof FullHttpRequest request) {
                handleHandshake(context, request);
                return;
            }
            if (message instanceof WebSocketFrame frame) {
                handleWebSocketFrame(context, frame);
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext context, Throwable cause) {
            logger.warn("WebSocket channel failure", cause);
            context.close();
        }
    }

    private enum ChannelRole {
        MARKET_DATA,
        ORDERS
    }

    private record SubscriptionEnvelope(String type, String symbol, String timestamp) {
    }

    private record ErrorEnvelope(String type, String message) {
    }

    private record MarketDataEnvelope(
            String type,
            String symbol,
            String asOf,
            List<LevelDto> bids,
            List<LevelDto> asks,
            List<TradeDto> trades) {
    }

    private record LevelDto(String price, long priceCents, long quantity, long cumulativeQuantity) {
    }

    private record TradeDto(
            long sequenceNumber,
            String symbol,
            String price,
            long priceCents,
            int quantity,
            String timestamp,
            String takerSide) {
    }

    /**
     * Flat JSON scanner for the high-frequency /ws/orders text path.
     *
     * <p>The order-entry payload is intentionally rigid and shallow. This scanner
     * extracts only the fields required by the gateway and avoids Gson tree
     * creation, maps, regex, BigDecimal parsing, or temporary enum strings.
     */
    private static final class OrderCommandJsonScanner {
        private static final String TYPE_KEY = "\"type\"";
        private static final String ORDER_ID_KEY = "\"orderId\"";
        private static final String CLIENT_ID_KEY = "\"clientId\"";
        private static final String SYMBOL_KEY = "\"symbol\"";
        private static final String SIDE_KEY = "\"side\"";
        private static final String ORDER_TYPE_KEY = "\"orderType\"";
        private static final String PRICE_KEY = "\"price\"";
        private static final String QUANTITY_KEY = "\"quantity\"";
        private static final String STP_MODE_KEY = "\"stpMode\"";

        private OrderCommandJsonScanner() {
        }

        private static boolean isNewOrder(String json) {
            int start = stringValueStart(json, TYPE_KEY, true);
            int end = stringValueEnd(json, start);
            return asciiEquals(json, start, end, "new-order");
        }

        private static String requiredString(String json, String key, String fieldName) {
            String value = optionalString(json, key);
            if (value == null || value.isEmpty()) {
                throw new IllegalArgumentException("Missing required field: " + fieldName);
            }
            return value;
        }

        private static String optionalString(String json, String key) {
            int valueStart = valueStart(json, key, false);
            if (valueStart < 0 || startsWithNull(json, valueStart)) {
                return null;
            }
            if (json.charAt(valueStart) != '"') {
                throw new IllegalArgumentException("Expected string field: " + key);
            }
            int start = valueStart + 1;
            int end = stringValueEnd(json, start);
            for (int i = start; i < end; i++) {
                if (json.charAt(i) == '\\') {
                    throw new IllegalArgumentException("Escaped JSON strings are not supported on the hot path");
                }
            }
            return json.substring(start, end);
        }

        private static Side side(String json) {
            int start = stringValueStart(json, SIDE_KEY, true);
            int end = stringValueEnd(json, start);
            if (asciiEquals(json, start, end, "BUY")) {
                return Side.BUY;
            }
            if (asciiEquals(json, start, end, "SELL")) {
                return Side.SELL;
            }
            throw new IllegalArgumentException("Unsupported side");
        }

        private static OrderType orderType(String json) {
            int start = stringValueStart(json, ORDER_TYPE_KEY, true);
            int end = stringValueEnd(json, start);
            if (asciiEquals(json, start, end, "LIMIT")) {
                return OrderType.LIMIT;
            }
            if (asciiEquals(json, start, end, "MARKET")) {
                return OrderType.MARKET;
            }
            if (asciiEquals(json, start, end, "IOC")) {
                return OrderType.IOC;
            }
            if (asciiEquals(json, start, end, "FOK")) {
                return OrderType.FOK;
            }
            if (asciiEquals(json, start, end, "STOP")) {
                return OrderType.STOP;
            }
            if (asciiEquals(json, start, end, "STOP_LIMIT") || asciiEquals(json, start, end, "STOP-LIMIT")) {
                return OrderType.STOP_LIMIT;
            }
            return unsupportedOrderType();
        }

        private static OrderType unsupportedOrderType() {
            throw new IllegalArgumentException("Unsupported order type");
        }

        private static SelfTradePreventionMode stpMode(String json) {
            int valueStart = valueStart(json, STP_MODE_KEY, false);
            if (valueStart < 0 || startsWithNull(json, valueStart)) {
                return SelfTradePreventionMode.CANCEL_NEWEST;
            }
            if (json.charAt(valueStart) != '"') {
                throw new IllegalArgumentException("Expected string field: stpMode");
            }
            int start = valueStart + 1;
            int end = stringValueEnd(json, start);
            if (asciiEquals(json, start, end, "CANCEL_NEWEST")) {
                return SelfTradePreventionMode.CANCEL_NEWEST;
            }
            if (asciiEquals(json, start, end, "CANCEL_OLDEST")) {
                return SelfTradePreventionMode.CANCEL_OLDEST;
            }
            if (asciiEquals(json, start, end, "DECREMENT_LARGER")) {
                return SelfTradePreventionMode.DECREMENT_LARGER;
            }
            return unsupportedStpMode();
        }

        private static SelfTradePreventionMode unsupportedStpMode() {
            throw new IllegalArgumentException("Unsupported self-trade prevention mode");
        }

        private static int quantity(String json) {
            int start = valueStart(json, QUANTITY_KEY, true);
            return parsePositiveInt(json, start, valueEnd(json, start));
        }

        private static long priceCents(String json) {
            int start = valueStart(json, PRICE_KEY, true);
            if (startsWithNull(json, start)) {
                throw new IllegalArgumentException("price is required for non-market orders");
            }
            if (json.charAt(start) == '"') {
                int contentStart = start + 1;
                int contentEnd = stringValueEnd(json, contentStart);
                return parsePriceCents(json, contentStart, contentEnd);
            }
            return parsePriceCents(json, start, valueEnd(json, start));
        }

        private static int stringValueStart(String json, String key, boolean required) {
            int valueStart = valueStart(json, key, required);
            if (valueStart < 0) {
                return -1;
            }
            if (json.charAt(valueStart) != '"') {
                throw new IllegalArgumentException("Expected string field: " + key);
            }
            return valueStart + 1;
        }

        private static int valueStart(String json, String key, boolean required) {
            int keyIndex = json.indexOf(key);
            if (keyIndex < 0) {
                if (required) {
                    throw new IllegalArgumentException("Missing required field: " + key);
                }
                return -1;
            }
            int colon = json.indexOf(':', keyIndex + key.length());
            if (colon < 0) {
                throw new IllegalArgumentException("Malformed JSON field: " + key);
            }
            int index = colon + 1;
            while (index < json.length() && Character.isWhitespace(json.charAt(index))) {
                index++;
            }
            if (index >= json.length()) {
                throw new IllegalArgumentException("Missing JSON field value: " + key);
            }
            return index;
        }

        private static int valueEnd(String json, int start) {
            int index = start;
            while (index < json.length()) {
                char c = json.charAt(index);
                if (c == ',' || c == '}' || Character.isWhitespace(c)) {
                    return index;
                }
                index++;
            }
            return index;
        }

        private static int stringValueEnd(String json, int start) {
            int end = json.indexOf('"', start);
            if (end < 0) {
                throw new IllegalArgumentException("Unterminated JSON string");
            }
            return end;
        }

        private static boolean startsWithNull(String json, int start) {
            return start + 4 <= json.length()
                    && json.charAt(start) == 'n'
                    && json.charAt(start + 1) == 'u'
                    && json.charAt(start + 2) == 'l'
                    && json.charAt(start + 3) == 'l';
        }

        private static int parsePositiveInt(String json, int start, int end) {
            if (start >= end) {
                throw new IllegalArgumentException("quantity must be positive");
            }
            int value = 0;
            for (int i = start; i < end; i++) {
                char c = json.charAt(i);
                if (c < '0' || c > '9') {
                    throw new IllegalArgumentException("quantity must be numeric");
                }
                value = Math.multiplyExact(value, 10);
                value = Math.addExact(value, c - '0');
            }
            if (value <= 0) {
                throw new IllegalArgumentException("quantity must be positive");
            }
            return value;
        }

        private static long parsePriceCents(String json, int start, int end) {
            if (start >= end) {
                throw new IllegalArgumentException("price is required");
            }
            long dollars = 0L;
            int cents = 0;
            int centDigits = 0;
            boolean decimal = false;
            for (int i = start; i < end; i++) {
                char c = json.charAt(i);
                if (c == '$' || c == ',') {
                    continue;
                }
                if (c == '.') {
                    if (decimal) {
                        throw new IllegalArgumentException("Invalid price value");
                    }
                    decimal = true;
                    continue;
                }
                if (c < '0' || c > '9') {
                    throw new IllegalArgumentException("Invalid price value");
                }
                int digit = c - '0';
                if (decimal) {
                    if (centDigits == 2) {
                        throw new IllegalArgumentException("Price precision exceeds cents");
                    }
                    cents = cents * 10 + digit;
                    centDigits++;
                } else {
                    dollars = Math.multiplyExact(dollars, 10L);
                    dollars = Math.addExact(dollars, digit);
                }
            }
            if (centDigits == 1) {
                cents *= 10;
            }
            long result = Math.addExact(Math.multiplyExact(dollars, 100L), cents);
            if (result <= 0L) {
                throw new IllegalArgumentException("Price must be positive");
            }
            return result;
        }

        private static boolean asciiEquals(String json, int start, int end, String expected) {
            int length = end - start;
            if (length != expected.length()) {
                return false;
            }
            for (int i = 0; i < length; i++) {
                char actual = json.charAt(start + i);
                char wanted = expected.charAt(i);
                if (actual == wanted) {
                    continue;
                }
                if (actual >= 'a' && actual <= 'z') {
                    actual = (char) (actual - 32);
                }
                if (wanted >= 'a' && wanted <= 'z') {
                    wanted = (char) (wanted - 32);
                }
                if (actual != wanted) {
                    return false;
                }
            }
            return true;
        }
    }

    private static final class ExecutionReportJsonEncoder {
        private static final ThreadLocal<StringBuilder> BUILDERS =
                ThreadLocal.withInitial(() -> new StringBuilder(512));

        private static ByteBuf encode(Channel channel, RingBufferEvent event) {
            StringBuilder builder = BUILDERS.get();
            builder.setLength(0);
            builder.append("{\"type\":\"execution-report\",\"status\":");
            appendJsonString(builder, status(event.getEventType()));
            builder.append(",\"sequenceNumber\":").append(event.getSequenceNumber())
                    .append(",\"orderId\":");
            appendJsonString(builder, event.getOrderId());
            builder.append(",\"clientId\":");
            appendJsonString(builder, event.getClientId());
            builder.append(",\"symbol\":");
            appendJsonString(builder, event.getSymbol());
            builder.append(",\"eventTimestamp\":");
            appendInstantEpochNanosString(builder, event.getEventTimestamp());
            builder.append(",\"details\":{");
            appendDetails(builder, event);
            builder.append("}}");

            ByteBuf buffer = channel.alloc().buffer(builder.length());
            buffer.writeCharSequence(builder, StandardCharsets.UTF_8);
            return buffer;
        }

        private static String status(RingBufferEvent.EventType eventType) {
            return switch (eventType) {
                case ACCEPTED -> "accepted";
                case REJECTED -> "rejected";
                case EXECUTED -> "executed";
                case CANCELLED -> "cancelled";
                case RESTATED -> "restated";
                case ADMIN -> "admin";
            };
        }

        private static void appendDetails(StringBuilder builder, RingBufferEvent event) {
            switch (event.getEventType()) {
                case ACCEPTED -> {
                    builder.append("\"side\":");
                    appendJsonString(builder, event.getSide() == null ? null : event.getSide().name());
                    builder.append(",\"orderType\":");
                    appendJsonString(builder, event.getOrderType() == null ? null : event.getOrderType().name());
                    builder.append(",\"price\":");
                    appendJsonPriceString(builder, event.getPrice());
                    builder.append(",\"quantity\":").append(event.getQuantity())
                            .append(",\"leavesQty\":").append(event.getLeavesQty())
                            .append(",\"cumQty\":").append(event.getCumQty());
                }
                case REJECTED -> {
                    builder.append("\"reason\":");
                    appendJsonString(builder, event.getRejectReason() == null ? null : event.getRejectReason().name());
                    builder.append(",\"message\":");
                    appendJsonString(builder, event.getMessage());
                }
                case EXECUTED -> {
                    builder.append("\"side\":");
                    appendJsonString(builder, event.getSide() == null ? null : event.getSide().name());
                    builder.append(",\"contraOrderId\":");
                    appendJsonString(builder, event.getContraOrderId());
                    builder.append(",\"fillPrice\":");
                    appendJsonPriceString(builder, event.getFillPrice());
                    builder.append(",\"fillQty\":").append(event.getFillQty())
                            .append(",\"leavesQty\":").append(event.getLeavesQty())
                            .append(",\"cumQty\":").append(event.getCumQty())
                            .append(",\"fullFill\":").append(event.isFullFill())
                            .append(",\"latencyNanos\":")
                            .append(Math.max(0L, event.getEventEmittedNanos() - event.getEngineInNanos()));
                }
                case CANCELLED -> {
                    builder.append("\"cancelledQty\":").append(event.getCancelledQty())
                            .append(",\"reason\":");
                    appendJsonString(builder, event.getMessage());
                }
                case RESTATED -> {
                    builder.append("\"price\":");
                    appendJsonPriceString(builder, event.getPrice());
                    builder.append(",\"quantity\":").append(event.getQuantity())
                            .append(",\"leavesQty\":").append(event.getLeavesQty())
                            .append(",\"cumQty\":").append(event.getCumQty());
                }
                case ADMIN -> {
                    builder.append("\"operation\":");
                    appendJsonString(builder, event.getAdminOperation() == null ? null : event.getAdminOperation().name());
                    builder.append(",\"message\":");
                    appendJsonString(builder, event.getMessage());
                }
            }
        }

        private static void appendJsonPriceString(StringBuilder builder, Price price) {
            if (price == null) {
                builder.append("null");
                return;
            }
            long cents = price.getCents();
            long absCents = Math.abs(cents);
            long dollars = absCents / 100;
            long fractional = absCents % 100;
            builder.append('"');
            if (cents < 0) {
                builder.append('-');
            }
            builder.append(dollars).append('.');
            if (fractional < 10) {
                builder.append('0');
            }
            builder.append(fractional).append('"');
        }

        private static void appendInstantEpochNanosString(StringBuilder builder, Instant instant) {
            if (instant == null) {
                builder.append("null");
                return;
            }
            builder.append('"')
                    .append(instant.getEpochSecond())
                    .append('.');
            int nanos = instant.getNano();
            int divisor = 100_000_000;
            while (divisor > 0) {
                builder.append((char) ('0' + (nanos / divisor) % 10));
                divisor /= 10;
            }
            builder.append('"');
        }
    }
}
