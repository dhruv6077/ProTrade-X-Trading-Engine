package sim;

import java.util.*;

/**
 * Comprehensive metrics for backtest analysis.
 * Calculates P&L, Sharpe ratio, maximum drawdown, and other performance metrics.
 */
public class BacktestMetrics {
    // Basic metrics
    private double totalReturn;
    private double annualizedReturn;
    private long totalTrades;
    private long winningTrades;
    private long losingTrades;
    
    // Risk metrics
    private double maxDrawdown;
    private double maxDrawdownPercent;
    private double sharpeRatio;
    private double volatility;
    private double sortinoRatio;
    private double calmarRatio;
    
    // Trade metrics
    private double winRate;
    private double profitFactor;
    private double averageWin;
    private double averageLoss;
    private double expectancy;
    
    // Execution metrics
    private double averageSlippage;
    private double commissionPaid;
    private long largestWin;
    private long largestLoss;
    private int consecutiveWins;
    private int consecutiveLosses;
    
    // Time period
    private long backtestDays;
    private long backtestHours;
    
    public BacktestMetrics() {
        // Initialize with defaults
        this.totalReturn = 0;
        this.annualizedReturn = 0;
        this.totalTrades = 0;
        this.sharpeRatio = 0;
        this.maxDrawdown = 0;
        this.winRate = 0;
    }
    
    // Getters
    public double getTotalReturn() { return totalReturn; }
    public void setTotalReturn(double val) { this.totalReturn = val; }
    
    public double getAnnualizedReturn() { return annualizedReturn; }
    public void setAnnualizedReturn(double val) { this.annualizedReturn = val; }
    
    public long getTotalTrades() { return totalTrades; }
    public void setTotalTrades(long val) { this.totalTrades = val; }
    
    public long getWinningTrades() { return winningTrades; }
    public void setWinningTrades(long val) { this.winningTrades = val; }
    
    public long getLosingTrades() { return losingTrades; }
    public void setLosingTrades(long val) { this.losingTrades = val; }
    
    public double getMaxDrawdown() { return maxDrawdown; }
    public void setMaxDrawdown(double val) { this.maxDrawdown = val; }
    
    public double getMaxDrawdownPercent() { return maxDrawdownPercent; }
    public void setMaxDrawdownPercent(double val) { this.maxDrawdownPercent = val; }
    
    public double getSharpeRatio() { return sharpeRatio; }
    public void setSharpeRatio(double val) { this.sharpeRatio = val; }
    
    public double getVolatility() { return volatility; }
    public void setVolatility(double val) { this.volatility = val; }
    
    public double getSortinoRatio() { return sortinoRatio; }
    public void setSortinoRatio(double val) { this.sortinoRatio = val; }
    
    public double getCalmarRatio() { return calmarRatio; }
    public void setCalmarRatio(double val) { this.calmarRatio = val; }
    
    public double getWinRate() { return winRate; }
    public void setWinRate(double val) { this.winRate = val; }
    
    public double getProfitFactor() { return profitFactor; }
    public void setProfitFactor(double val) { this.profitFactor = val; }
    
    public double getAverageWin() { return averageWin; }
    public void setAverageWin(double val) { this.averageWin = val; }
    
    public double getAverageLoss() { return averageLoss; }
    public void setAverageLoss(double val) { this.averageLoss = val; }
    
    public double getExpectancy() { return expectancy; }
    public void setExpectancy(double val) { this.expectancy = val; }
    
    public long getBacktestDays() { return backtestDays; }
    public void setBacktestDays(long val) { this.backtestDays = val; }
    
    /**
     * Generate a formatted report of metrics.
     */
    public String generateReport() {
        StringBuilder sb = new StringBuilder();
        sb.append("\n========== BACKTEST RESULTS ==========\n");
        sb.append(String.format("Total Return: %.2f%%\n", totalReturn * 100));
        sb.append(String.format("Annualized Return: %.2f%%\n", annualizedReturn * 100));
        sb.append(String.format("Max Drawdown: %.2f%%\n", maxDrawdownPercent));
        sb.append(String.format("Sharpe Ratio: %.2f\n", sharpeRatio));
        sb.append(String.format("Sortino Ratio: %.2f\n", sortinoRatio));
        sb.append(String.format("Calmar Ratio: %.2f\n", calmarRatio));
        sb.append(String.format("\nTotal Trades: %d\n", totalTrades));
        sb.append(String.format("Winning Trades: %d\n", winningTrades));
        sb.append(String.format("Losing Trades: %d\n", losingTrades));
        sb.append(String.format("Win Rate: %.2f%%\n", winRate * 100));
        sb.append(String.format("Profit Factor: %.2f\n", profitFactor));
        sb.append(String.format("Average Win: %.2f\n", averageWin));
        sb.append(String.format("Average Loss: %.2f\n", averageLoss));
        sb.append(String.format("Expectancy: %.2f\n", expectancy));
        sb.append(String.format("\nBacktest Period: %d days\n", backtestDays));
        sb.append("=====================================\n");
        return sb.toString();
    }
}
