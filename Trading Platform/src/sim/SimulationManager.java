package sim;

import User.ProductManager;
import exchange.core.ExchangeRuntime;
import exchange.marketdata.feed.MarketDataFeedBootstrap;
import exchange.model.OrderType;
import exchange.model.SelfTradePreventionMode;
import exchange.model.Side;
import exchange.risk.InMemoryRiskEngine;
import logging.AuditEvent;
import logging.AuditEventType;
import logging.AuditLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import persistence.DatabaseManager;
import Price.PriceFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Manages the lifecycle of the trading simulation.
 */
public class SimulationManager {
    private static final Logger logger = LoggerFactory.getLogger(SimulationManager.class);
    private static final int WEBSOCKET_PORT = 9090;
    private static final long MARKET_MAKER_BUYING_POWER_CENTS = 500_000_000_00L;
    private static final long MARKET_MAKER_INVENTORY_SHARES = 0L;

    private static SimulationManager instance;

    private ExecutorService executorService;
    private final List<BaseBot> takerBots;
    private final List<MarketMakerBot> marketMakers;
    private boolean isRunning = false;

    private SimulationManager() {
        this.executorService = Executors.newCachedThreadPool();
        this.takerBots = new ArrayList<>();
        this.marketMakers = new ArrayList<>();
    }

    public static synchronized SimulationManager getInstance() {
        if (instance == null) {
            instance = new SimulationManager();
        }
        return instance;
    }

    public void initialize() {
        try {
            try {
                DatabaseManager.getInstance().initialize();
                logger.info("Database initialized");
            } catch (Exception e) {
                logger.warn("Database init failed (using file logs)");
            }

            List<String> defaultSymbols = List.of("AAPL", "GOOGL", "MSFT", "TSLA", "AMZN");
            ExchangeRuntime runtime = ExchangeRuntime.getInstance();
            List<String> symbolList = MarketDataFeedBootstrap.bootstrapConfiguredFeed(
                    runtime, ProductManager.getInstance(), defaultSymbols);

            runtime.bootstrapFromPostgresIfAvailable();

            InMemoryRiskEngine riskEngine = runtime.riskEngine();
            seedInitialDepth(runtime, riskEngine, symbolList);

            for (String symbol : symbolList) {
                long referenceMidCents = SimulatedMarketConfig.referenceMidCents(symbol);
                marketMakers.add(createMarketMaker(riskEngine, "MM_1_" + symbol, List.of(symbol),
                        5L, 50, referenceMidCents));
                marketMakers.add(createMarketMaker(riskEngine, "MM_2_" + symbol, List.of(symbol),
                        8L, 40, referenceMidCents));
            }

            riskEngine.setShortSellingEnabled("TRADER_A", true);
            riskEngine.setShortSellingEnabled("TRADER_B", true);
            riskEngine.setShortSellingEnabled("WHALE_1", true);
            takerBots.add(new LiquidityTakerBot("TRADER_A", symbolList, 500));
            takerBots.add(new LiquidityTakerBot("TRADER_B", symbolList, 800));
            takerBots.add(new LiquidityTakerBot("WHALE_1", symbolList, 2000));

            logger.info("Simulation initialized with {} market makers and {} taker bots",
                    marketMakers.size(), takerBots.size());

            AuditLogger.getInstance().logEvent(new AuditEvent.Builder()
                    .eventType(AuditEventType.SYSTEM_START)
                    .addData("version", "3.0.0")
                    .build());

        } catch (Exception e) {
            logger.error("Failed to initialize simulation", e);
        }
    }

    private MarketMakerBot createMarketMaker(
            InMemoryRiskEngine riskEngine,
            String clientId,
            List<String> symbols,
            long halfSpreadCents,
            int orderSize,
            long defaultMidPriceCents) {
        WebSocketMarketMakerSimulator.fundMarketMaker(riskEngine, clientId, symbols,
                MARKET_MAKER_BUYING_POWER_CENTS, MARKET_MAKER_INVENTORY_SHARES);
        MarketMakerConfig config = MarketMakerConfig.builder()
                .clientId(clientId)
                .symbols(symbols)
                .halfSpreadCents(halfSpreadCents)
                .orderSize(orderSize)
                .defaultMidPriceCents(defaultMidPriceCents)
                .webSocketPort(WEBSOCKET_PORT)
                .build();
        return new MarketMakerBot(config);
    }

    private void seedInitialDepth(
            ExchangeRuntime runtime,
            InMemoryRiskEngine riskEngine,
            List<String> symbols) {
        for (String symbol : symbols) {
            String seedClient = "SEED_MM_" + symbol;
            riskEngine.setAvailableCash(seedClient, MARKET_MAKER_BUYING_POWER_CENTS);
            riskEngine.setShortSellingEnabled(seedClient, true);
            riskEngine.setPosition(seedClient, symbol, MARKET_MAKER_INVENTORY_SHARES);

            long mid = SimulatedMarketConfig.referenceMidCents(symbol);
            for (int level = 1; level <= 5; level++) {
                long offset = level * 5L;
                int quantity = 100 + (level * 25);
                submitSeedOrder(runtime, seedClient, symbol, Side.BUY, mid - offset, quantity, level);
                submitSeedOrder(runtime, seedClient, symbol, Side.SELL, mid + offset, quantity, level);
            }
        }
    }

    private void submitSeedOrder(
            ExchangeRuntime runtime,
            String clientId,
            String symbol,
            Side side,
            long priceCents,
            int quantity,
            int level) {
        try {
            runtime.gateway().submitNewOrder(
                    "SEED-" + symbol + "-" + side + "-" + level,
                    clientId,
                    symbol,
                    side,
                    OrderType.LIMIT,
                    PriceFactory.makePrice(priceCents),
                    quantity,
                    SelfTradePreventionMode.CANCEL_NEWEST);
        } catch (Exception e) {
            logger.warn("Failed to seed {} {} level {}: {}", symbol, side, level, e.getMessage());
        }
    }

    public void start() {
        if (isRunning) {
            return;
        }
        isRunning = true;

        if (executorService.isShutdown()) {
            executorService = Executors.newCachedThreadPool();
        }

        logger.info("Starting simulation...");
        for (MarketMakerBot marketMaker : marketMakers) {
            try {
                marketMaker.start();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error("Failed to start market maker {}", marketMaker.config().clientId(), e);
            }
        }
        for (BaseBot bot : takerBots) {
            bot.reset();
            executorService.submit(bot);
        }
    }

    public void stop() {
        if (!isRunning) {
            return;
        }
        isRunning = false;

        logger.info("Stopping simulation...");
        for (MarketMakerBot marketMaker : marketMakers) {
            marketMaker.close();
        }
        for (BaseBot bot : takerBots) {
            bot.stop();
        }

        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
        }

        AuditLogger.getInstance().logEvent(new AuditEvent.Builder()
                .eventType(AuditEventType.SYSTEM_SHUTDOWN)
                .build());

        /*
         * This method is wired to the dashboard's Start/Stop simulation controls.
         * Stopping the simulation should pause bot activity only. The database
         * connection pool is application-level infrastructure and is closed by
         * the runtime shutdown hook; closing it here makes a later Start behave
         * like a partial application restart and breaks audit persistence.
         */
    }
}
