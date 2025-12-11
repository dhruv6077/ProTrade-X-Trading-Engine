package persistence;

import java.util.List;
import java.util.Map;

/**
 * Represents a snapshot of engine state at a point in time.
 * Used for state replication and failover recovery.
 */
public class EngineStateSnapshot {
    public long timestamp;
    public List<String> openOrders;
    public List<String> completedTrades;
    public Map<String, Long> executedVolume;
    public double totalEquity;
    
    public EngineStateSnapshot(long timestamp, List<String> openOrders,
                              List<String> trades, Map<String, Long> volume, double equity) {
        this.timestamp = timestamp;
        this.openOrders = openOrders;
        this.completedTrades = trades;
        this.executedVolume = volume;
        this.totalEquity = equity;
    }
    
    @Override
    public String toString() {
        return String.format("StateSnapshot{ts=%d, orders=%d, trades=%d, equity=%.2f}",
            timestamp, openOrders.size(), completedTrades.size(), totalEquity);
    }
}
