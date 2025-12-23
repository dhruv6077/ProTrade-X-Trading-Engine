package monitoring;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Monitors system performance metrics, particularly order processing latency
 */
public class PerformanceMonitor {
    private static PerformanceMonitor instance;

    // Track latency for the last N operations
    private final ConcurrentLinkedQueue<Long> latencyHistory = new ConcurrentLinkedQueue<>();
    private static final int MAX_HISTORY_SIZE = 100;

    // Running counters
    private final AtomicLong totalOperations = new AtomicLong(0);
    private final AtomicLong totalLatencyNanos = new AtomicLong(0);

    private PerformanceMonitor() {
    }

    public static synchronized PerformanceMonitor getInstance() {
        if (instance == null) {
            instance = new PerformanceMonitor();
        }
        return instance;
    }

    /**
     * Record a latency measurement in nanoseconds
     */
    public void recordLatency(long latencyNanos) {
        // Update running totals
        totalOperations.incrementAndGet();
        totalLatencyNanos.addAndGet(latencyNanos);

        // Maintain history
        latencyHistory.offer(latencyNanos);
        if (latencyHistory.size() > MAX_HISTORY_SIZE) {
            latencyHistory.poll();
        }
    }

    /**
     * Get average latency over recent operations in milliseconds
     */
    public double getRecentAverageLatencyMs() {
        if (latencyHistory.isEmpty()) {
            return 0.0;
        }

        long sum = 0;
        int count = 0;
        for (Long latency : latencyHistory) {
            sum += latency;
            count++;
        }

        return count > 0 ? (sum / (double) count) / 1_000_000.0 : 0.0;
    }

    /**
     * Get overall average latency in milliseconds
     */
    public double getOverallAverageLatencyMs() {
        long ops = totalOperations.get();
        if (ops == 0) {
            return 0.0;
        }
        return totalLatencyNanos.get() / (double) ops / 1_000_000.0;
    }

    /**
     * Get total number of operations tracked
     */
    public long getTotalOperations() {
        return totalOperations.get();
    }

    /**
     * Reset all metrics
     */
    public void reset() {
        latencyHistory.clear();
        totalOperations.set(0);
        totalLatencyNanos.set(0);
    }

    /**
     * Helper class to time operations
     */
    public static class Timer {
        private final long startTime;

        public Timer() {
            this.startTime = System.nanoTime();
        }

        /**
         * Stop the timer and record the latency
         */
        public void stop() {
            long elapsed = System.nanoTime() - startTime;
            PerformanceMonitor.getInstance().recordLatency(elapsed);
        }

        /**
         * Get elapsed time in nanoseconds without recording
         */
        public long getElapsedNanos() {
            return System.nanoTime() - startTime;
        }
    }
}
