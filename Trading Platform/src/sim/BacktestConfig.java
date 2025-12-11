package sim;

import java.time.LocalDate;
import java.util.List;

/**
 * Configuration for backtesting runs.
 * Specifies date range, initial capital, products, and strategy parameters.
 */
public class BacktestConfig {
    private LocalDate startDate;
    private LocalDate endDate;
    private List<String> products;
    private double initialCapital;
    private TradingStrategy strategy;
    private double transactionCostBps;  // Basis points
    private double slippageBps;         // Basis points for slippage simulation
    private boolean includeCommissions;

    public BacktestConfig(LocalDate startDate, LocalDate endDate, 
                         List<String> products, double initialCapital,
                         TradingStrategy strategy) {
        this.startDate = startDate;
        this.endDate = endDate;
        this.products = products;
        this.initialCapital = initialCapital;
        this.strategy = strategy;
        this.transactionCostBps = 1.0;  // 1 basis point default
        this.slippageBps = 0.5;          // 0.5 basis points default
        this.includeCommissions = true;
    }

    // Getters and Setters
    public LocalDate getStartDate() { return startDate; }
    public void setStartDate(LocalDate startDate) { this.startDate = startDate; }

    public LocalDate getEndDate() { return endDate; }
    public void setEndDate(LocalDate endDate) { this.endDate = endDate; }

    public List<String> getProducts() { return products; }
    public void setProducts(List<String> products) { this.products = products; }

    public double getInitialCapital() { return initialCapital; }
    public void setInitialCapital(double initialCapital) { this.initialCapital = initialCapital; }

    public TradingStrategy getStrategy() { return strategy; }
    public void setStrategy(TradingStrategy strategy) { this.strategy = strategy; }

    public double getTransactionCostBps() { return transactionCostBps; }
    public void setTransactionCostBps(double bps) { this.transactionCostBps = bps; }

    public double getSlippageBps() { return slippageBps; }
    public void setSlippageBps(double bps) { this.slippageBps = bps; }

    public boolean isIncludeCommissions() { return includeCommissions; }
    public void setIncludeCommissions(boolean include) { this.includeCommissions = include; }
}
