package exchange.telemetry;

import org.HdrHistogram.ConcurrentHistogram;
import org.HdrHistogram.Histogram;

import java.util.concurrent.TimeUnit;

/**
 * High-resolution latency recorder for the exchange hot path.
 *
 * <p>Recording is safe from Netty, Disruptor, and shard threads. Snapshotting is
 * a cold-path operation used by diagnostics and tests.</p>
 */
public final class LatencyTelemetry {
    private static final long HIGHEST_TRACKABLE_NS = TimeUnit.SECONDS.toNanos(60);
    private static final int SIGNIFICANT_DIGITS = 3;
    private static final LatencyTelemetry INSTANCE = new LatencyTelemetry();

    private final ConcurrentHistogram tickToTradeNs =
            new ConcurrentHistogram(1L, HIGHEST_TRACKABLE_NS, SIGNIFICANT_DIGITS);
    private final ConcurrentHistogram engineToDispatchNs =
            new ConcurrentHistogram(1L, HIGHEST_TRACKABLE_NS, SIGNIFICANT_DIGITS);
    private final ConcurrentHistogram egressWriteNs =
            new ConcurrentHistogram(1L, HIGHEST_TRACKABLE_NS, SIGNIFICANT_DIGITS);

    public static LatencyTelemetry getInstance() {
        return INSTANCE;
    }

    public void recordEngineToDispatch(long ingressTimeNs, long eventEmittedNs) {
        record(engineToDispatchNs, ingressTimeNs, eventEmittedNs);
    }

    public void recordTickToTrade(long ingressTimeNs, long egressWriteNs) {
        record(tickToTradeNs, ingressTimeNs, egressWriteNs);
    }

    public void recordEgressWrite(long eventTimestampNs, long egressWriteNs) {
        record(this.egressWriteNs, eventTimestampNs, egressWriteNs);
    }

    public LatencyReport snapshot() {
        return new LatencyReport(
                snapshotOf(tickToTradeNs),
                snapshotOf(engineToDispatchNs),
                snapshotOf(egressWriteNs));
    }

    public void reset() {
        tickToTradeNs.reset();
        engineToDispatchNs.reset();
        egressWriteNs.reset();
    }

    private static void record(ConcurrentHistogram histogram, long startNs, long endNs) {
        if (startNs <= 0L || endNs < startNs) {
            return;
        }
        long delta = endNs - startNs;
        if (delta <= 0L) {
            delta = 1L;
        }
        histogram.recordValue(Math.min(delta, HIGHEST_TRACKABLE_NS));
    }

    private static LatencySnapshot snapshotOf(Histogram source) {
        Histogram copy = source.copy();
        return new LatencySnapshot(
                copy.getTotalCount(),
                copy.getMinValue(),
                copy.getValueAtPercentile(50.0),
                copy.getValueAtPercentile(95.0),
                copy.getValueAtPercentile(99.0),
                copy.getValueAtPercentile(99.9),
                copy.getValueAtPercentile(99.99),
                copy.getMaxValue());
    }

    public record LatencyReport(
            LatencySnapshot tickToTrade,
            LatencySnapshot engineToDispatch,
            LatencySnapshot egressWrite) {
    }

    public record LatencySnapshot(
            long count,
            long minNs,
            long p50Ns,
            long p95Ns,
            long p99Ns,
            long p999Ns,
            long p9999Ns,
            long maxNs) {
    }
}
