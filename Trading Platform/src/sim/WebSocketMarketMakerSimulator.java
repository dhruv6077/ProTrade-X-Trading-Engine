package sim;

import exchange.core.ExchangeRuntime;
import exchange.marketdata.L2Snapshot;
import exchange.marketdata.MarketDataEngine;
import exchange.risk.InMemoryRiskEngine;
import exchange.ws.NettyWebSocketServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Standalone runner that spins up the exchange WebSocket API and multiple {@link MarketMakerBot}
 * instances to stress-test the Netty layer and keep every symbol two-sided.
 *
 * <p>Usage: {@code java sim.WebSocketMarketMakerSimulator [wsPort] [botCount]}</p>
 */
public final class WebSocketMarketMakerSimulator {
    private static final Logger logger = LoggerFactory.getLogger(WebSocketMarketMakerSimulator.class);
    private static final String[] DEFAULT_SYMBOLS = { "AAPL", "GOOGL", "MSFT", "TSLA", "AMZN" };
    private static final long DEFAULT_BUYING_POWER_CENTS = 1_000_000_000_00L;
    private static final long DEFAULT_INVENTORY_SHARES = 0L;

    private WebSocketMarketMakerSimulator() {
    }

    public static void main(String[] args) throws Exception {
        int wsPort = args.length > 0 ? Integer.parseInt(args[0]) : 9090;
        int botCount = args.length > 1 ? Integer.parseInt(args[1]) : 5;

        ExchangeRuntime runtime = ExchangeRuntime.getInstance();
        runtime.addSymbols(List.of(DEFAULT_SYMBOLS));
        runtime.bootstrapFromPostgresIfAvailable();

        NettyWebSocketServer webSocketServer = new NettyWebSocketServer(runtime, wsPort);
        webSocketServer.start();

        InMemoryRiskEngine riskEngine = runtime.riskEngine();
        List<MarketMakerBot> bots = launchBots(riskEngine, wsPort, botCount, List.of(DEFAULT_SYMBOLS));

        ScheduledExecutorService monitor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "book-depth-monitor");
            thread.setDaemon(true);
            return thread;
        });
        monitor.scheduleAtFixedRate(() -> verifyBookDepth(runtime.marketDataEngine(), List.of(DEFAULT_SYMBOLS)),
                1, 1, TimeUnit.SECONDS);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> shutdown(bots, monitor, webSocketServer),
                "ws-mm-simulator-shutdown"));

        logger.info("WebSocket market maker simulator running with {} bots on ws://localhost:{}/ws/*",
                botCount, wsPort);
        logger.info("Press Ctrl+C to stop");

        Thread.currentThread().join();
    }

    static List<MarketMakerBot> launchBots(
            InMemoryRiskEngine riskEngine,
            int wsPort,
            int botCount,
            List<String> symbols) throws InterruptedException {
        List<MarketMakerBot> bots = new ArrayList<>(Math.max(1, botCount) * symbols.size());

        for (int index = 0; index < botCount; index++) {
            long halfSpreadCents = 5L + (index * 2L);
            int orderSize = 25 + (index * 10);

            for (String symbol : symbols) {
                String clientId = "MM_" + (index + 1) + "_" + symbol;
                long defaultMid = SimulatedMarketConfig.referenceMidCents(symbol);

                fundMarketMaker(riskEngine, clientId, List.of(symbol),
                        DEFAULT_BUYING_POWER_CENTS, DEFAULT_INVENTORY_SHARES);

                MarketMakerConfig config = MarketMakerConfig.builder()
                        .clientId(clientId)
                        .symbols(List.of(symbol))
                        .halfSpreadCents(halfSpreadCents)
                        .orderSize(orderSize)
                        .defaultMidPriceCents(defaultMid)
                        .webSocketPort(wsPort)
                        .build();

                MarketMakerBot bot = new MarketMakerBot(config);
                bot.start();
                bots.add(bot);
            }
        }
        return bots;
    }

    public static void fundMarketMaker(
            InMemoryRiskEngine riskEngine,
            String clientId,
            List<String> symbols,
            long buyingPowerCents,
            long inventoryShares) {
        riskEngine.setAvailableCash(clientId, buyingPowerCents);
        riskEngine.setShortSellingEnabled(clientId, true);
        for (String symbol : symbols) {
            riskEngine.setPosition(clientId, symbol, inventoryShares);
        }
    }

    static void verifyBookDepth(MarketDataEngine marketDataEngine, List<String> symbols) {
        for (String symbol : symbols) {
            L2Snapshot snapshot = marketDataEngine.l2Snapshot(symbol, 1);
            if (snapshot.bids().isEmpty() || snapshot.asks().isEmpty()) {
                logger.warn("Thin book detected for {} (bids={}, asks={}) — bots should replenish shortly",
                        symbol, snapshot.bids().size(), snapshot.asks().size());
            }
        }
    }

    private static void shutdown(List<MarketMakerBot> bots, ScheduledExecutorService monitor,
            NettyWebSocketServer webSocketServer) {
        monitor.shutdownNow();
        for (MarketMakerBot bot : bots) {
            bot.close();
        }
        webSocketServer.close();
    }
}
