package logging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

/**
 * Monitors and tracks order processing latency across the system.
 * Maintains statistics and alerts on latency violations.
 */
public class LatencyMonitor {
    private static final Logger logger = LoggerFactory.getLogger(LatencyMonitor.class);
    private static final LatencyMonitor instance = new LatencyMonitor();
    
    // Recent timelines (keep last N)
    private final Queue<OrderProcessingTimeline> recentTimelines;
    private final int maxCapacity;
    
    // Statistics tracking
    private final List<Long> latencies;
    private long totalProcessedOrders;
    
    // Latency violations tracking
    private final Queue<OrderProcessingTimeline.LatencyViolation> violations;
    
    private LatencyMonitor() {
        this.maxCapacity = 10000;
        this.recentTimelines = new ConcurrentLinkedQueue<>();
        this.latencies = Collections.synchronizedList(new ArrayList<>());
        this.violations = new ConcurrentLinkedQueue<>();
        this.totalProcessedOrders = 0;
    }
    
    public static LatencyMonitor getInstance() {
        return instance;
    }
    
    /**
     * Record a completed order processing timeline.
     */
    public void recordTimeline(OrderProcessingTimeline timeline) {
        if (timeline == null) {
            return;
        }
        
        long latency = timeline.getEndToEndLatency();
        latencies.add(latency);
        recentTimelines.offer(timeline);
        totalProcessedOrders++;
        
        // Keep only recent N timelines
        if (recentTimelines.size() > maxCapacity) {
            recentTimelines.poll();
        }
        
        // Check for violations
        OrderProcessingTimeline.LatencyViolation violation = timeline.checkThresholds();
        if (violation != null) {
            violations.offer(violation);
            if (violations.size() > 1000) {
                violations.poll();
            }
            logger.warn("Latency violation: {}", violation);
        }
    }
    
    /**
     * Get comprehensive latency statistics.
     */
    public LatencyStats getStats() {
        if (latencies.isEmpty()) {
            return new LatencyStats(0, 0, 0, 0, 0, 0, 0);
        }
        
        List<Long> sorted = new ArrayList<>(latencies);
        Collections.sort(sorted);
        
        return new LatencyStats(
            sorted.get(0),                                    // Min
            sorted.get(sorted.size() - 1),                    // Max
            (long) sorted.stream().mapToLong(Long::longValue).average().orElse(0),  // Mean
            getPercentile(sorted, 50),                        // P50 (median)
            getPercentile(sorted, 95),                        // P95
            getPercentile(sorted, 99),                        // P99
            getPercentile(sorted, 99.9)                       // P99.9
        );
    }
    
    /**
     * Calculate percentile from sorted data.
     */
    private long getPercentile(List<Long> sorted, double percentile) {
        if (sorted.isEmpty()) return 0;
        int index = (int) Math.ceil((percentile / 100.0) * sorted.size()) - 1;
        index = Math.max(0, Math.min(index, sorted.size() - 1));
        return sorted.get(index);
    }
    
    /**
     * Get violation statistics.
     */
    public ViolationStats getViolationStats() {
        long count = violations.size();
        double violationRate = totalProcessedOrders > 0 ? 
            (count / (double) totalProcessedOrders) * 100 : 0;
        
        return new ViolationStats(count, totalProcessedOrders, violationRate);
    }
    
    /**
     * Export detailed metrics report.
     */
    public String generateReport() {
        LatencyStats latencyStats = getStats();
        ViolationStats violationStats = getViolationStats();
        
        StringBuilder sb = new StringBuilder();
        sb.append("\n========== LATENCY MONITOR REPORT ==========\n");
        sb.append(String.format("Total Orders Processed: %d\n", totalProcessedOrders));
        sb.append(String.format("\nLatency Percentiles (nanoseconds):\n"));
        sb.append(String.format("  Min:   %,d ns (%.2f µs)\n", latencyStats.min, latencyStats.min / 1000.0));
        sb.append(String.format("  P50:   %,d ns (%.2f µs)\n", latencyStats.p50, latencyStats.p50 / 1000.0));
        sb.append(String.format("  P95:   %,d ns (%.2f µs)\n", latencyStats.p95, latencyStats.p95 / 1000.0));
        sb.append(String.format("  P99:   %,d ns (%.2f µs)\n", latencyStats.p99, latencyStats.p99 / 1000.0));
        sb.append(String.format("  P99.9: %,d ns (%.2f µs)\n", latencyStats.p99_9, latencyStats.p99_9 / 1000.0));
        sb.append(String.format("  Max:   %,d ns (%.2f µs)\n", latencyStats.max, latencyStats.max / 1000.0));
        
        sb.append(String.format("\nViolations:\n"));
        sb.append(String.format("  Count: %d\n", violationStats.violationCount));
        sb.append(String.format("  Rate:  %.4f%%\n", violationStats.violationRate));
        
        sb.append("\n==========================================\n");
        return sb.toString();
    }
    
    /**
     * Reset all statistics.
     */
    public void reset() {
        recentTimelines.clear();
        violations.clear();
        latencies.clear();
        totalProcessedOrders = 0;
        logger.info("Latency monitor reset");
    }
    
    /**
     * Get recent violations.
     */
    public List<OrderProcessingTimeline.LatencyViolation> getRecentViolations(int limit) {
        List<OrderProcessingTimeline.LatencyViolation> recent = new ArrayList<>();
        int count = 0;
        for (OrderProcessingTimeline.LatencyViolation v : violations) {
            if (count++ >= limit) break;
            recent.add(v);
        }
        Collections.reverse(recent);
        return recent;
    }
    
    /**
     * Latency statistics holder.
     */
    public static class LatencyStats {
        public final long min;
        public final long max;
        public final long mean;
        public final long p50;
        public final long p95;
        public final long p99;
        public final long p99_9;
        
        public LatencyStats(long min, long max, long mean, long p50, long p95, long p99, long p99_9) {
            this.min = min;
            this.max = max;
            this.mean = mean;
            this.p50 = p50;
            this.p95 = p95;
            this.p99 = p99;
            this.p99_9 = p99_9;
        }
    }
    
    /**
     * Violation statistics holder.
     */
    public static class ViolationStats {
        public final long violationCount;
        public final long totalOrders;
        public final double violationRate;
        
        public ViolationStats(long violations, long total, double rate) {
            this.violationCount = violations;
            this.totalOrders = total;
            this.violationRate = rate;
        }
    }
}
