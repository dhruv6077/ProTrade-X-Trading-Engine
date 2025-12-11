package sim;

import java.util.List;

/**
 * Complete backtest result containing metrics, trades, and portfolio state.
 */
public class BacktestResult {
    private final BacktestMetrics metrics;
    private final List<BacktestEngine.BacktestTrade> trades;
    private final SimulatedPortfolio finalPortfolio;
    private final long executionTimeMs;
    
    public BacktestResult(BacktestMetrics metrics, 
                         List<BacktestEngine.BacktestTrade> trades,
                         SimulatedPortfolio portfolio) {
        this.metrics = metrics;
        this.trades = trades;
        this.finalPortfolio = portfolio;
        this.executionTimeMs = System.currentTimeMillis();
    }
    
    // Getters
    public BacktestMetrics getMetrics() { return metrics; }
    public List<BacktestEngine.BacktestTrade> getTrades() { return trades; }
    public SimulatedPortfolio getPortfolio() { return finalPortfolio; }
    public long getExecutionTimeMs() { return executionTimeMs; }
    
    /**
     * Generate a comprehensive backtest report.
     */
    public String generateFullReport() {
        StringBuilder sb = new StringBuilder();
        sb.append(metrics.generateReport());
        sb.append("\n========== SAMPLE TRADES ==========\n");
        
        int sampleSize = Math.min(10, trades.size());
        for (int i = 0; i < sampleSize; i++) {
            sb.append(trades.get(i)).append("\n");
        }
        
        if (trades.size() > sampleSize) {
            sb.append(String.format("... and %d more trades\n", trades.size() - sampleSize));
        }
        
        sb.append("====================================\n");
        return sb.toString();
    }
}
