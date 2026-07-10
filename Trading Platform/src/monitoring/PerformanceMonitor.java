package monitoring;

import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Monitors system performance metrics, particularly order processing latency
 */
public final class PerformanceMonitor {
    private static final int MAX_HISTORY_SIZE = 128;
    private static final int HISTORY_MASK = MAX_HISTORY_SIZE - 1;

    private final AtomicLongArray latencyHistory = new AtomicLongArray(MAX_HISTORY_SIZE);
    private final AtomicLong writeSequence = new AtomicLong();
    private final AtomicLong totalOperations = new AtomicLong();
    private final AtomicLong totalLatencyNanos = new AtomicLong();

    private PerformanceMonitor() {
    }

    public static PerformanceMonitor getInstance() {
        return Holder.INSTANCE;
    }

    /**
     * Record a latency measurement in nanoseconds
     */
    public void recordLatency(long latencyNanos) {
        if (latencyNanos < 0) {
            return;
        }
        totalOperations.incrementAndGet();
        totalLatencyNanos.addAndGet(latencyNanos);
        long sequence = writeSequence.getAndIncrement();
        latencyHistory.set((int) (sequence & HISTORY_MASK), latencyNanos);
    }

    /**
     * Get average latency over recent operations in milliseconds
     */
    public double getRecentAverageLatencyMs() {
        long writes = writeSequence.get();
        int count = (int) Math.min(writes, MAX_HISTORY_SIZE);
        if (count == 0) {
            return 0.0;
        }

        long sum = 0;
        for (int i = 0; i < count; i++) {
            sum += latencyHistory.get(i);
        }

        return (sum / (double) count) / 1_000_000.0;
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
        for (int i = 0; i < MAX_HISTORY_SIZE; i++) {
            latencyHistory.set(i, 0L);
        }
        writeSequence.set(0);
        totalOperations.set(0);
        totalLatencyNanos.set(0);
    }

    /**
     * Helper class to time operations
     */
    public static final class Timer {
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

    private static final class Holder {
        private static final PerformanceMonitor INSTANCE = new PerformanceMonitor();
    }
}
