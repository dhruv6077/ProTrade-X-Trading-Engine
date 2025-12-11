package sim;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.*;

/**
 * Calculates comprehensive financial metrics for backtest results.
 * Includes return metrics, risk metrics, and trade quality metrics.
 */
public class MetricsCalculator {
    private static final Logger logger = LoggerFactory.getLogger(MetricsCalculator.class);
    private static final double RISK_FREE_RATE = 0.02 / 252;  // 2% annual
    
    /**
     * Calculate all metrics from backtest data.
     */
    public BacktestMetrics calculate(List<BacktestEngine.BacktestTrade> trades,
                                    SimulatedPortfolio portfolio,
                                    double initialCapital,
                                    LocalDate startDate,
                                    LocalDate endDate) {
        BacktestMetrics metrics = new BacktestMetrics();
        
        // Basic return metrics
        double finalValue = portfolio.getEquity();
        metrics.setTotalReturn((finalValue - initialCapital) / initialCapital);
        
        long days = ChronoUnit.DAYS.between(startDate, endDate);
        double years = days / 365.0;
        metrics.setBacktestDays(days);
        
        if (years > 0) {
            metrics.setAnnualizedReturn(Math.pow(1 + metrics.getTotalReturn(), 1.0 / years) - 1);
        }
        
        // Risk metrics
        metrics.setMaxDrawdown(portfolio.getMaxDrawdown());
        metrics.setMaxDrawdownPercent(portfolio.getMaxDrawdownPercent());
        
        // Trade analysis
        if (!trades.isEmpty()) {
            analyzeTrades(trades, metrics);
        }
        
        // Volatility and risk-adjusted returns
        List<Double> dailyReturns = calculateDailyReturns(trades);
        if (!dailyReturns.isEmpty()) {
            // Calculate standard deviation manually
            double volatility = calculateStandardDeviation(dailyReturns);
            metrics.setVolatility(volatility);
            metrics.setSharpeRatio(calculateSharpeRatio(metrics.getTotalReturn(), 
                                                       volatility, years));
            metrics.setSortinoRatio(calculateSortinoRatio(dailyReturns, years));
        }
        
        // Calmar Ratio = Annual Return / Max Drawdown
        if (metrics.getMaxDrawdownPercent() > 0) {
            metrics.setCalmarRatio(metrics.getAnnualizedReturn() / metrics.getMaxDrawdownPercent());
        }
        
        logger.info("Metrics calculated: Return={:.2f}%, Sharpe={:.2f}, DD={:.2f}%",
            metrics.getTotalReturn() * 100,
            metrics.getSharpeRatio(),
            metrics.getMaxDrawdownPercent());
        
        return metrics;
    }
    
    /**
     * Analyze trade statistics.
     */
    private void analyzeTrades(List<BacktestEngine.BacktestTrade> trades, BacktestMetrics metrics) {
        metrics.setTotalTrades(trades.size());
        
        double totalWinPnL = 0;
        double totalLossPnL = 0;
        int wins = 0;
        int losses = 0;
        long largestWin = 0;
        long largestLoss = 0;
        
        List<Double> pnlList = new ArrayList<>();
        
        for (int i = 0; i < trades.size() - 1; i++) {
            BacktestEngine.BacktestTrade entry = trades.get(i);
            BacktestEngine.BacktestTrade exit = trades.get(i + 1);
            
            // Calculate P&L (simplified: assumes entry/exit pairs)
            if (!entry.side.equals(exit.side)) {
                double pnl = entry.quantity * (exit.price - entry.price);
                pnlList.add(pnl);
                
                if (pnl > 0) {
                    wins++;
                    totalWinPnL += pnl;
                    largestWin = Math.max(largestWin, (long) pnl);
                } else {
                    losses++;
                    totalLossPnL += pnl;
                    largestLoss = Math.min(largestLoss, (long) pnl);
                }
            }
        }
        
        metrics.setWinningTrades(wins);
        metrics.setLosingTrades(losses);
        
        if (metrics.getTotalTrades() > 0) {
            metrics.setWinRate((double) wins / metrics.getTotalTrades());
        }
        
        if (wins > 0) {
            metrics.setAverageWin(totalWinPnL / wins);
        }
        
        if (losses > 0) {
            metrics.setAverageLoss(totalLossPnL / losses);
        }
        
        // Profit Factor
        if (Math.abs(totalLossPnL) > 0) {
            metrics.setProfitFactor(totalWinPnL / Math.abs(totalLossPnL));
        }
        
        // Expectancy
        metrics.setExpectancy(
            (metrics.getWinRate() * metrics.getAverageWin()) -
            ((1 - metrics.getWinRate()) * Math.abs(metrics.getAverageLoss()))
        );
    }
    
    /**
     * Calculate daily returns from trades.
     */
    private List<Double> calculateDailyReturns(List<BacktestEngine.BacktestTrade> trades) {
        Map<Long, Double> dailyPnL = new TreeMap<>();
        
        // Group trades by day and calculate daily PnL
        for (int i = 0; i < trades.size() - 1; i++) {
            BacktestEngine.BacktestTrade trade = trades.get(i);
            long day = trade.timestamp / (24 * 60 * 60 * 1000);  // Convert to day
            
            double pnl = trade.quantity * (trades.get(i + 1).price - trade.price);
            dailyPnL.put(day, dailyPnL.getOrDefault(day, 0.0) + pnl);
        }
        
        return new ArrayList<>(dailyPnL.values());
    }
    
    /**
     * Calculate Sharpe Ratio = (Return - RiskFreeRate) / Volatility
     */
    private double calculateSharpeRatio(double totalReturn, double volatility, double years) {
        if (volatility == 0) {
            return 0;
        }
        
        double annualizedReturn = Math.pow(1 + totalReturn, 1.0 / Math.max(years, 1)) - 1;
        return (annualizedReturn - RISK_FREE_RATE) / volatility;
    }
    
    /**
     * Calculate standard deviation of returns.
     */
    private double calculateStandardDeviation(List<Double> returns) {
        if (returns.isEmpty()) {
            return 0;
        }
        
        double mean = returns.stream().mapToDouble(Double::doubleValue).average().orElse(0);
        double sumSquaredDiff = 0;
        for (Double ret : returns) {
            sumSquaredDiff += (ret - mean) * (ret - mean);
        }
        
        return Math.sqrt(sumSquaredDiff / returns.size());
    }
    
    /**
     * Calculate Sortino Ratio = (Return - RiskFreeRate) / DownsideDeviation
     */
    private double calculateSortinoRatio(List<Double> dailyReturns, double years) {
        if (dailyReturns.isEmpty()) {
            return 0;
        }
        
        // Calculate downside deviation (only negative returns)
        double sumDownsideSquared = 0;
        for (Double ret : dailyReturns) {
            if (ret < 0) {
                sumDownsideSquared += ret * ret;
            }
        }
        
        double downsideDeviation = Math.sqrt(sumDownsideSquared / dailyReturns.size());
        if (downsideDeviation == 0) {
            return 0;
        }
        
        double avgReturn = dailyReturns.stream().mapToDouble(Double::doubleValue).average().orElse(0);
        return (avgReturn - RISK_FREE_RATE) / downsideDeviation;
    }
}
