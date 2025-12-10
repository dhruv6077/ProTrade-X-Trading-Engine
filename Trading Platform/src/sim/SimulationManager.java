package sim;

import User.ProductManager;
import logging.AuditEvent;
import logging.AuditEventType;
import logging.AuditLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import persistence.DatabaseManager;

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
    private static SimulationManager instance;

    private ExecutorService executorService;
    private final List<BaseBot> bots;
    private boolean isRunning = false;

    private SimulationManager() {
        this.executorService = Executors.newCachedThreadPool();
        this.bots = new ArrayList<>();
    }

    public static synchronized SimulationManager getInstance() {
        if (instance == null) {
            instance = new SimulationManager();
        }
        return instance;
    }

    public void initialize() {
        try {
            // Initialize Database
            try {
                DatabaseManager.getInstance().initialize();
                logger.info("Database initialized");
            } catch (Exception e) {
                logger.warn("Database init failed (using file logs)");
            }

            // Initialize Products
            String[] products = { "AAPL", "GOOGL", "MSFT", "TSLA", "AMZN" };
            for (String p : products) {
                try {
                    ProductManager.getInstance().addProduct(p);
                } catch (Exception e) {
                    // Ignore if already exists
                }
            }

            // Create Bots
            bots.add(new MarketMakerBot("MM_1", Arrays.asList(products), 1000));
            bots.add(new MarketMakerBot("MM_2", Arrays.asList(products), 1200));
            bots.add(new LiquidityTakerBot("TRADER_A", Arrays.asList(products), 500));
            bots.add(new LiquidityTakerBot("TRADER_B", Arrays.asList(products), 800));
            bots.add(new LiquidityTakerBot("WHALE_1", Arrays.asList(products), 2000));

            logger.info("Simulation initialized with {} bots", bots.size());

            // Log System Start
            AuditLogger.getInstance().logEvent(new AuditEvent.Builder()
                    .eventType(AuditEventType.SYSTEM_START)
                    .addData("version", "3.0.0")
                    .build());

        } catch (Exception e) {
            logger.error("Failed to initialize simulation", e);
        }
    }

    public void start() {
        if (isRunning)
            return;
        isRunning = true;

        // Recreate executor if it was shut down
        if (executorService.isShutdown()) {
            executorService = Executors.newCachedThreadPool();
        }

        logger.info("Starting simulation...");
        for (BaseBot bot : bots) {
            bot.reset(); // Reset bot state
            executorService.submit(bot);
        }
    }

    public void stop() {
        if (!isRunning)
            return;
        isRunning = false;

        logger.info("Stopping simulation...");
        for (BaseBot bot : bots) {
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

        // Log System Shutdown
        AuditLogger.getInstance().logEvent(new AuditEvent.Builder()
                .eventType(AuditEventType.SYSTEM_SHUTDOWN)
                .build());

        if (DatabaseManager.getInstance().isInitialized()) {
            DatabaseManager.getInstance().shutdown();
        }
    }
}
