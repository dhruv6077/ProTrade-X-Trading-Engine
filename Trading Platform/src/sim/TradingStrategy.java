package sim;

import logging.AuditEvent;

/**
 * Interface for trading strategies used in backtesting and real-time trading.
 * Strategies implement decision logic based on market conditions and portfolio state.
 */
public interface TradingStrategy {
    
    /**
     * Called on each market data update to determine trading actions.
     * 
     * @param auditEvent Market event (trade, quote, etc.)
     * @param portfolio Current portfolio state
     * @return StrategyAction indicating what to do (buy, sell, hold)
     */
    StrategyAction onMarketEvent(AuditEvent auditEvent, SimulatedPortfolio portfolio);
    
    /**
     * Initialize strategy with configuration.
     */
    void initialize();
    
    /**
     * Get strategy name.
     */
    String getName();
    
    /**
     * Get strategy parameters for reporting.
     */
    String getParameters();
    
    /**
     * Enumeration for strategy actions.
     */
    enum StrategyAction {
        BUY,
        SELL,
        HOLD,
        CLOSE_POSITION
    }
}
