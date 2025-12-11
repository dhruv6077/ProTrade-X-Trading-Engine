package sim;

import logging.AuditEvent;
import logging.AuditEventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Core backtesting engine that replays historical market data through a trading strategy.
 * Produces detailed metrics on strategy performance.
 */
public class BacktestEngine {
    private static final Logger logger = LoggerFactory.getLogger(BacktestEngine.class);
    
    private final HistoricalDataLoader dataLoader;
    private final MetricsCalculator metricsCalculator;
    private final List<BacktestTrade> trades;
    
    public BacktestEngine() {
        this.dataLoader = new HistoricalDataLoader();
        this.metricsCalculator = new MetricsCalculator();
        this.trades = new ArrayList<>();
    }
    
    /**
     * Run a complete backtest with the given configuration.
     * 
     * @param config Backtest configuration (dates, strategy, products, capital)
     * @return BacktestResult with detailed metrics and trade list
     */
    public BacktestResult runBacktest(BacktestConfig config) {
        logger.info("Starting backtest: {} to {}", config.getStartDate(), config.getEndDate());
        long startTime = System.currentTimeMillis();
        
        trades.clear();
        
        // 1. Load historical data
        List<AuditEvent> historicalEvents = dataLoader.loadEvents(
            config.getStartDate(),
            config.getEndDate(),
            config.getProducts()
        );
        
        if (historicalEvents.isEmpty()) {
            logger.warn("No historical data found for backtest period");
            return new BacktestResult(
                new BacktestMetrics(),
                trades,
                new SimulatedPortfolio(config.getInitialCapital())
            );
        }
        
        // 2. Initialize portfolio and strategy
        SimulatedPortfolio portfolio = new SimulatedPortfolio(config.getInitialCapital());
        config.getStrategy().initialize();
        
        // 3. Replay historical events
        logger.info("Replaying {} historical events through strategy", historicalEvents.size());
        for (AuditEvent event : historicalEvents) {
            // Only process relevant market events
            if (isRelevantEvent(event)) {
                TradingStrategy.StrategyAction action = 
                    config.getStrategy().onMarketEvent(event, portfolio);
                
                // Execute action if needed
                handleStrategyAction(action, event, portfolio, config);
            }
        }
        
        // 4. Close any open positions at end of backtest
        portfolio.getPositions().forEach(pos -> {
            // In real backtest, would use last price from data
            portfolio.closePosition(pos.getSymbol(), pos.getQuantity(), 100.0);
        });
        
        // 5. Calculate metrics
        BacktestMetrics metrics = metricsCalculator.calculate(
            trades,
            portfolio,
            config.getInitialCapital(),
            config.getStartDate(),
            config.getEndDate()
        );
        
        long elapsed = System.currentTimeMillis() - startTime;
        logger.info("Backtest completed in {} ms. Result: Total Return = {:.2f}%, Sharpe = {:.2f}",
            elapsed, metrics.getTotalReturn() * 100, metrics.getSharpeRatio());
        
        return new BacktestResult(metrics, trades, portfolio);
    }
    
    /**
     * Check if event is relevant for trading decisions (TRADE_EXECUTED, MARKET_UPDATE).
     */
    private boolean isRelevantEvent(AuditEvent event) {
        if (event == null || event.getEventType() == null) {
            return false;
        }
        
        String eventType = event.getEventType().name();
        return eventType.equals("TRADE_EXECUTED") || 
               eventType.equals("MARKET_UPDATE") ||
               eventType.equals("ORDER_FILLED");
    }
    
    /**
     * Execute trading action from strategy.
     */
    private void handleStrategyAction(TradingStrategy.StrategyAction action,
                                     AuditEvent event,
                                     SimulatedPortfolio portfolio,
                                     BacktestConfig config) {
        if (action == null || action == TradingStrategy.StrategyAction.HOLD) {
            return;
        }
        
        String product = event.getProduct();
        if (product == null) {
            return;
        }
        
        // Extract price from event data (simplified)
        double price = extractPriceFromEvent(event);
        long quantity = 100;  // Fixed position size for simplicity
        
        switch (action) {
            case BUY:
                portfolio.addPosition(product, quantity, price, "BUY");
                logger.debug("BUY signal for {} @ {}", product, price);
                recordTrade(product, quantity, price, "BUY", event.getTimestamp().toEpochMilli());
                break;
                
            case SELL:
                SimulatedPortfolio.Position pos = portfolio.getPosition(product);
                if (pos != null && pos.getQuantity() > 0) {
                    portfolio.closePosition(product, Math.min(pos.getQuantity(), quantity), price);
                    logger.debug("SELL signal for {} @ {}", product, price);
                    recordTrade(product, quantity, price, "SELL", event.getTimestamp().toEpochMilli());
                }
                break;
                
            case CLOSE_POSITION:
                SimulatedPortfolio.Position openPos = portfolio.getPosition(product);
                if (openPos != null && openPos.getQuantity() > 0) {
                    portfolio.closePosition(product, openPos.getQuantity(), price);
                    logger.debug("CLOSE position for {}", product);
                }
                break;
        }
    }
    
    /**
     * Extract price from audit event data.
     * Looks for "price" or "lastPrice" field in event data JSON.
     */
    private double extractPriceFromEvent(AuditEvent event) {
        try {
            if (event.getData() instanceof Map) {
                Map<String, Object> data = (Map<String, Object>) event.getData();
                if (data.containsKey("price")) {
                    Object price = data.get("price");
                    if (price instanceof Number) {
                        return ((Number) price).doubleValue();
                    } else if (price instanceof String) {
                        String priceStr = (String) price;
                        if (priceStr.startsWith("$")) {
                            return Double.parseDouble(priceStr.substring(1));
                        }
                        return Double.parseDouble(priceStr);
                    }
                }
                if (data.containsKey("lastPrice")) {
                    return ((Number) data.get("lastPrice")).doubleValue();
                }
            }
        } catch (Exception e) {
            logger.debug("Failed to extract price from event", e);
        }
        
        return 100.0;  // Default price
    }
    
    /**
     * Record a trade for reporting.
     */
    private void recordTrade(String symbol, long quantity, double price, 
                            String side, long timestamp) {
        BacktestTrade trade = new BacktestTrade(symbol, quantity, price, side, timestamp);
        trades.add(trade);
    }
    
    /**
     * Represents a single trade executed during backtest.
     */
    public static class BacktestTrade {
        public String symbol;
        public long quantity;
        public double price;
        public String side;
        public long timestamp;
        public double pnl;
        
        public BacktestTrade(String symbol, long qty, double price, String side, long timestamp) {
            this.symbol = symbol;
            this.quantity = qty;
            this.price = price;
            this.side = side;
            this.timestamp = timestamp;
        }
        
        @Override
        public String toString() {
            return String.format("%s %d %s @ %.2f on %d", side, quantity, symbol, price, timestamp);
        }
    }
}
