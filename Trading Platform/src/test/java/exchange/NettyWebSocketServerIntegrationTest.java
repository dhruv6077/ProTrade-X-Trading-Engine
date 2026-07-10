package exchange;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import Price.PriceFactory;
import exchange.marketdata.TradeRecord;
import exchange.model.OrderType;
import exchange.model.SelfTradePreventionMode;
import exchange.model.Side;
import exchange.ws.BinaryOrderCodec;
import exchange.ws.NettyWebSocketServer;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketClientProtocolHandler;
import io.netty.handler.codec.http.websocketx.WebSocketClientProtocolHandler.ClientHandshakeStateEvent;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketVersion;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NettyWebSocketServerIntegrationTest {

    @Test
    void streamsMarketDataAndExecutionReportsOverWebSocket() throws Exception {
        try (ExchangeTestSupport.TestExchange exchange = ExchangeTestSupport.newExchange(java.util.Set.of("AAPL"))) {
            exchange.riskEngine().setAvailableCash("WS_BUYER", 1_000_000);
            NettyWebSocketServer server = new NettyWebSocketServer(
                    exchange.gateway(),
                    exchange.dispatcher(),
                    exchange.marketDataEngine(),
                    0);
            server.start();

            WebSocket marketSocket = null;
            WebSocket orderSocket = null;
            try {
                HttpClient client = HttpClient.newHttpClient();
                MessageCollector marketCollector = new MessageCollector();
                MessageCollector orderCollector = new MessageCollector();

                marketSocket = client.newWebSocketBuilder()
                        .connectTimeout(Duration.ofSeconds(5))
                        .buildAsync(URI.create("ws://localhost:" + server.port() + "/ws/market-data"), marketCollector)
                        .join();
                orderSocket = client.newWebSocketBuilder()
                        .connectTimeout(Duration.ofSeconds(5))
                        .buildAsync(URI.create("ws://localhost:" + server.port() + "/ws/orders"), orderCollector)
                        .join();

                marketSocket.sendText("""
                        {"type":"subscribe","symbol":"AAPL"}
                        """, true).join();

                orderSocket.sendText("""
                        {"type":"new-order","orderId":"WS-ORDER-1","clientId":"WS_BUYER","symbol":"AAPL","side":"BUY","orderType":"LIMIT","price":"10.00","quantity":25}
                        """, true).join();

                assertTrue(orderCollector.await(message -> {
                    JsonObject payload = parse(message);
                    return "execution-report".equals(stringValue(payload, "type"))
                            && "accepted".equals(stringValue(payload, "status"))
                            && "WS-ORDER-1".equals(stringValue(payload, "orderId"));
                }, Duration.ofSeconds(5)));

                assertTrue(marketCollector.await(message -> {
                    JsonObject payload = parse(message);
                    if (!"l2".equals(stringValue(payload, "type")) || !"AAPL".equals(stringValue(payload, "symbol"))) {
                        return false;
                    }
                    if (!payload.has("bids") || payload.getAsJsonArray("bids").isEmpty()) {
                        return false;
                    }
                    JsonObject bestBid = payload.getAsJsonArray("bids").get(0).getAsJsonObject();
                    return "10.00".equals(stringValue(bestBid, "price"))
                            && bestBid.get("quantity").getAsLong() == 25L;
                }, Duration.ofSeconds(5)));
            } finally {
                if (marketSocket != null) {
                    marketSocket.sendClose(WebSocket.NORMAL_CLOSURE, "done").join();
                }
                if (orderSocket != null) {
                    orderSocket.sendClose(WebSocket.NORMAL_CLOSURE, "done").join();
                }
                server.close();
            }
        }
    }

    @Test
    void acceptsBinaryOrderFramesOverWebSocket() throws Exception {
        try (ExchangeTestSupport.TestExchange exchange = ExchangeTestSupport.newExchange(java.util.Set.of("AAPL"))) {
            exchange.riskEngine().setAvailableCash("WS_BIN_BUYER", 1_000_000);
            NettyWebSocketServer server = new NettyWebSocketServer(
                    exchange.gateway(),
                    exchange.dispatcher(),
                    exchange.marketDataEngine(),
                    0);
            server.start();

            WebSocket orderSocket = null;
            try {
                HttpClient client = HttpClient.newHttpClient();
                MessageCollector orderCollector = new MessageCollector();

                orderSocket = client.newWebSocketBuilder()
                        .connectTimeout(Duration.ofSeconds(5))
                        .buildAsync(URI.create("ws://localhost:" + server.port() + "/ws/orders"), orderCollector)
                        .join();

                ByteBuffer binaryOrder = BinaryOrderCodec.encodeNewOrder(
                        "WS-BIN-ORDER-1",
                        "WS_BIN_BUYER",
                        "AAPL",
                        Side.BUY,
                        OrderType.LIMIT,
                        10_00,
                        25,
                        SelfTradePreventionMode.CANCEL_NEWEST);
                orderSocket.sendBinary(binaryOrder, true).join();

                assertTrue(orderCollector.await(message -> {
                    JsonObject payload = parse(message);
                    return "execution-report".equals(stringValue(payload, "type"))
                            && "accepted".equals(stringValue(payload, "status"))
                            && "WS-BIN-ORDER-1".equals(stringValue(payload, "orderId"));
                }, Duration.ofSeconds(5)));
            } finally {
                if (orderSocket != null) {
                    orderSocket.sendClose(WebSocket.NORMAL_CLOSURE, "done").join();
                }
                server.close();
            }
        }
    }

    @Test
    void disconnectsSlowMarketDataConsumerWithoutBlockingOrderIngress() throws Exception {
        try (ExchangeTestSupport.TestExchange exchange = ExchangeTestSupport.newExchange(java.util.Set.of("AAPL"))) {
            exchange.riskEngine().setAvailableCash("BACKPRESSURE_BUYER", 10_000_000);
            NettyWebSocketServer server = new NettyWebSocketServer(
                    exchange.gateway(),
                    exchange.dispatcher(),
                    exchange.marketDataEngine(),
                    0);
            server.start();

            SlowWebSocketClient slowClient = new SlowWebSocketClient(
                    URI.create("ws://localhost:" + server.port() + "/ws/market-data"),
                    "{\"type\":\"subscribe\",\"symbol\":\"AAPL\"}");
            try {
                slowClient.connect();
                assertTrue(slowClient.awaitSubscribed(Duration.ofSeconds(5)));

                List<TradeRecord> largeTradeTape = largeTradeTape();
                for (int i = 0; i < 500 && server.marketDataSubscriberCount("AAPL") > 0; i++) {
                    server.onTradeTape("AAPL", largeTradeTape);
                }
                await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                        assertEquals(0, server.marketDataSubscriberCount("AAPL"),
                                "server should evict a market-data client that stops reading"));

                int acceptedOrders = 1_000;
                for (int i = 0; i < acceptedOrders; i++) {
                    exchange.gateway().submitNewOrderAsync(
                            "BP-ORDER-" + i,
                            "BACKPRESSURE_BUYER",
                            "AAPL",
                            Side.BUY,
                            OrderType.LIMIT,
                            PriceFactory.makePrice(10_00 + i),
                            1,
                            SelfTradePreventionMode.CANCEL_NEWEST);
                }

                await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                        assertTrue(exchange.dispatcher().events().stream()
                                .filter(event -> event.clientId().equals("BACKPRESSURE_BUYER"))
                                .count() >= acceptedOrders));
                assertEquals(acceptedOrders, exchange.journal().replay().stream()
                        .filter(command -> command.clientId().equals("BACKPRESSURE_BUYER"))
                        .count());
            } finally {
                slowClient.close();
                server.close();
            }
        }
    }

    private static JsonObject parse(String message) {
        return JsonParser.parseString(message).getAsJsonObject();
    }

    private static String stringValue(JsonObject payload, String key) {
        return payload.has(key) && !payload.get(key).isJsonNull() ? payload.get(key).getAsString() : null;
    }

    private static List<TradeRecord> largeTradeTape() throws Exception {
        ArrayList<TradeRecord> trades = new ArrayList<>(2_048);
        for (int i = 0; i < 2_048; i++) {
            trades.add(new TradeRecord(i, "AAPL", PriceFactory.makePrice(10_00 + (i % 25)), 100 + i,
                    Instant.EPOCH.plusNanos(i), Side.BUY));
        }
        return List.copyOf(trades);
    }

    private static final class MessageCollector implements WebSocket.Listener {
        private final BlockingQueue<String> messages = new LinkedBlockingQueue<>();
        private final CompletableFuture<Void> opened = new CompletableFuture<>();

        @Override
        public void onOpen(WebSocket webSocket) {
            opened.complete(null);
            webSocket.request(1);
        }

        @Override
        public java.util.concurrent.CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
            if (last) {
                messages.offer(data.toString());
            }
            webSocket.request(1);
            return null;
        }

        @Override
        public java.util.concurrent.CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
            opened.complete(null);
            return null;
        }

        @Override
        public void onError(WebSocket webSocket, Throwable error) {
            opened.completeExceptionally(error);
        }

        private boolean await(Predicate<String> matcher, Duration timeout) throws Exception {
            opened.get(5, TimeUnit.SECONDS);
            long deadline = System.nanoTime() + timeout.toNanos();
            while (System.nanoTime() < deadline) {
                String message = messages.poll(100, TimeUnit.MILLISECONDS);
                if (message != null && matcher.test(message)) {
                    return true;
                }
            }
            return false;
        }
    }

    private static final class SlowWebSocketClient implements AutoCloseable {
        private final URI uri;
        private final String subscribeMessage;
        private final EventLoopGroup group = new NioEventLoopGroup(1);
        private final CountDownLatch subscribed = new CountDownLatch(1);
        private final CountDownLatch closed = new CountDownLatch(1);
        private volatile Channel channel;

        private SlowWebSocketClient(URI uri, String subscribeMessage) {
            this.uri = uri;
            this.subscribeMessage = subscribeMessage;
        }

        private void connect() {
            Bootstrap bootstrap = new Bootstrap()
                    .group(group)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel channel) {
                            channel.pipeline()
                                    .addLast(new HttpClientCodec())
                                    .addLast(new HttpObjectAggregator(65_536))
                                    .addLast(new WebSocketClientProtocolHandler(uri, WebSocketVersion.V13, null,
                                            true, new DefaultHttpHeaders(), 65_536))
                                    .addLast(new SimpleChannelInboundHandler<WebSocketFrame>() {
                                        @Override
                                        public void userEventTriggered(ChannelHandlerContext context, Object event) {
                                            if (event == ClientHandshakeStateEvent.HANDSHAKE_COMPLETE) {
                                                context.writeAndFlush(new TextWebSocketFrame(subscribeMessage));
                                            }
                                        }

                                        @Override
                                        protected void channelRead0(ChannelHandlerContext context, WebSocketFrame frame) {
                                            if (frame instanceof TextWebSocketFrame) {
                                                subscribed.countDown();
                                                context.channel().config().setAutoRead(false);
                                            }
                                        }

                                        @Override
                                        public void channelInactive(ChannelHandlerContext context) {
                                            closed.countDown();
                                        }

                                        @Override
                                        public void exceptionCaught(ChannelHandlerContext context, Throwable cause) {
                                            closed.countDown();
                                            context.close();
                                        }
                                    });
                        }
                    });
            ChannelFuture future = bootstrap.connect(uri.getHost(), uri.getPort()).syncUninterruptibly();
            channel = future.channel();
            channel.closeFuture().addListener(ignored -> closed.countDown());
        }

        private boolean awaitSubscribed(Duration timeout) throws InterruptedException {
            return subscribed.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
        }

        private boolean awaitClosed(Duration timeout) throws InterruptedException {
            return closed.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
        }

        @Override
        public void close() {
            Channel current = channel;
            if (current != null) {
                current.close().syncUninterruptibly();
            }
            group.shutdownGracefully().syncUninterruptibly();
        }
    }
}
