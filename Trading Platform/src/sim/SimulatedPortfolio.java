package sim;

import logging.AuditEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simulated portfolio for backtesting.
 * Tracks positions, cash, and P&L during strategy simulation.
 */
public class SimulatedPortfolio {
    private static final Logger logger = LoggerFactory.getLogger(SimulatedPortfolio.class);
    
    private final double initialCash;
    private double cash;
    private double totalEquity;
    private double highWaterMark;
    private double maxDrawdown;
    
    // Position tracking
    private final java.util.Map<String, Position> positions;
    
    public SimulatedPortfolio(double initialCapital) {
        this.initialCash = initialCapital;
        this.cash = initialCapital;
        this.totalEquity = initialCapital;
        this.highWaterMark = initialCapital;
        this.maxDrawdown = 0;
        this.positions = new java.util.concurrent.ConcurrentHashMap<>();
    }
    
    /**
     * Add a position or increase existing position.
     */
    public void addPosition(String symbol, long quantity, double entryPrice, String side) {
        Position pos = positions.computeIfAbsent(symbol, k -> new Position(symbol));
        pos.addTrade(quantity, entryPrice, side);
        
        // Deduct cost from cash
        double cost = quantity * entryPrice;
        this.cash -= cost;
        updateEquity();
    }
    
    /**
     * Close a position partially or fully.
     */
    public void closePosition(String symbol, long quantity, double exitPrice) {
        Position pos = positions.get(symbol);
        if (pos == null) {
            logger.warn("Attempting to close non-existent position: {}", symbol);
            return;
        }
        
        double proceeds = quantity * exitPrice;
        this.cash += proceeds;
        pos.closeTrade(quantity, exitPrice);
        
        if (pos.getQuantity() == 0) {
            positions.remove(symbol);
        }
        
        updateEquity();
    }
    
    /**
     * Get position for a symbol.
     */
    public Position getPosition(String symbol) {
        return positions.get(symbol);
    }
    
    /**
     * Update total equity based on current positions and cash.
     */
    private void updateEquity() {
        double positionValue = positions.values().stream()
            .mapToDouble(Position::getUnrealizedPnL)
            .sum();
        
        this.totalEquity = cash + positionValue;
        
        // Update high water mark and max drawdown
        if (totalEquity > highWaterMark) {
            highWaterMark = totalEquity;
        }
        maxDrawdown = Math.max(maxDrawdown, highWaterMark - totalEquity);
    }
    
    // Getters
    public double getCash() { return cash; }
    public double getEquity() { return totalEquity; }
    public double getInitialCash() { return initialCash; }
    public double getHighWaterMark() { return highWaterMark; }
    public double getMaxDrawdown() { return maxDrawdown; }
    public double getMaxDrawdownPercent() { 
        return highWaterMark > 0 ? (maxDrawdown / highWaterMark) * 100 : 0; 
    }
    public java.util.Collection<Position> getPositions() { return positions.values(); }
    
    /**
     * Inner class representing a single position in a security.
     */
    public static class Position {
        private final String symbol;
        private long quantity;
        private double entryPrice;
        private double unrealizedPnL;
        private double realizedPnL;
        private java.util.List<Trade> trades;
        
        public Position(String symbol) {
            this.symbol = symbol;
            this.quantity = 0;
            this.entryPrice = 0;
            this.trades = new java.util.ArrayList<>();
        }
        
        public void addTrade(long qty, double price, String side) {
            this.trades.add(new Trade(qty, price, side, System.currentTimeMillis()));
            this.quantity += "BUY".equals(side) ? qty : -qty;
            
            // Recalculate average entry price
            double totalCost = 0;
            long totalQty = 0;
            for (Trade t : trades) {
                if ("BUY".equals(t.side)) {
                    totalCost += t.quantity * t.price;
                    totalQty += t.quantity;
                }
            }
            this.entryPrice = totalQty > 0 ? totalCost / totalQty : 0;
        }
        
        public void closeTrade(long qty, double exitPrice) {
            // Calculate realized P&L
            double pnl = qty * (exitPrice - entryPrice);
            this.realizedPnL += pnl;
            this.quantity -= qty;
        }
        
        public double getUnrealizedPnL() {
            return quantity * entryPrice;
        }
        
        public long getQuantity() { return quantity; }
        public String getSymbol() { return symbol; }
        public double getEntryPrice() { return entryPrice; }
        public double getRealizedPnL() { return realizedPnL; }
        
        public static class Trade {
            public long quantity;
            public double price;
            public String side;
            public long timestamp;
            
            public Trade(long qty, double price, String side, long timestamp) {
                this.quantity = qty;
                this.price = price;
                this.side = side;
                this.timestamp = timestamp;
            }
        }
    }
}
