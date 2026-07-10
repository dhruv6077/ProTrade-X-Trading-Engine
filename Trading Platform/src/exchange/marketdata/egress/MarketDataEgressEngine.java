package exchange.marketdata.egress;

import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.TimeoutException;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import exchange.core.AffinityThreadFactory;
import exchange.core.FailSafeDisruptorExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Downstream market-data egress ring.
 *
 * <p>Publisher thread: matching/MDE hot path, single writer, no locks.
 * Consumer thread: one or more egress handlers that encode and enqueue writes
 * onto Netty event loops. Network backpressure stays outside the execution
 * hot path.</p>
 */
public final class MarketDataEgressEngine implements AutoCloseable {
    public static final int DEFAULT_RING_BUFFER_SIZE = 65_536;

    private static final Logger logger = LoggerFactory.getLogger(MarketDataEgressEngine.class);

    private final Disruptor<MarketDataDeltaEvent> disruptor;
    private final RingBuffer<MarketDataDeltaEvent> ringBuffer;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    @SafeVarargs
    public MarketDataEgressEngine(EventHandler<MarketDataDeltaEvent>... handlers) {
        this(DEFAULT_RING_BUFFER_SIZE, WaitMode.YIELDING, new AffinityThreadFactory("market-data-egress"), handlers);
    }

    @SafeVarargs
    public MarketDataEgressEngine(
            int ringBufferSize,
            WaitMode waitMode,
            ThreadFactory threadFactory,
            EventHandler<MarketDataDeltaEvent>... handlers) {
        validatePowerOfTwo(ringBufferSize);
        Objects.requireNonNull(waitMode, "waitMode");
        Objects.requireNonNull(threadFactory, "threadFactory");
        if (handlers == null || handlers.length == 0) {
            throw new IllegalArgumentException("At least one market-data egress handler is required");
        }
        Arrays.stream(handlers).forEach(handler -> Objects.requireNonNull(handler, "handler"));

        WaitStrategy waitStrategy = switch (waitMode) {
            case YIELDING -> new YieldingWaitStrategy();
            case BUSY_SPIN -> new BusySpinWaitStrategy();
        };

        disruptor = new Disruptor<>(
                MarketDataDeltaEvent.Factory.INSTANCE,
                ringBufferSize,
                threadFactory,
                ProducerType.SINGLE,
                waitStrategy);
        disruptor.setDefaultExceptionHandler(new FailSafeDisruptorExceptionHandler<>("market-data-egress"));
        disruptor.handleEventsWith(handlers);
        ringBuffer = disruptor.start();
    }

    /**
     * Publishes a primitive L2/trade delta without allocating command or event
     * wrapper objects. Only the single configured publisher thread should call
     * this method.
     */
    public void publishDelta(
            long sequenceId,
            long symbolId,
            long price,
            long size,
            long cumulativeSize,
            byte side,
            byte updateType,
            long timestampNs) {
        publishDelta(sequenceId, symbolId, price, size, cumulativeSize, side, updateType, timestampNs, 0L);
    }

    public void publishDelta(
            long sequenceId,
            long symbolId,
            long price,
            long size,
            long cumulativeSize,
            byte side,
            byte updateType,
            long timestampNs,
            long ingressTimeNs) {
        if (closed.get()) {
            throw new IllegalStateException("Market data egress engine is closed");
        }
        long sequence = ringBuffer.next();
        try {
            MarketDataDeltaEvent event = ringBuffer.get(sequence);
            event.set(sequenceId, symbolId, price, size, cumulativeSize, side, updateType, timestampNs, ingressTimeNs);
        } finally {
            ringBuffer.publish(sequence);
        }
    }

    public long cursor() {
        return ringBuffer.getCursor();
    }

    public int bufferSize() {
        return ringBuffer.getBufferSize();
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        try {
            disruptor.shutdown(5, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            logger.warn("Market data egress ring did not drain within timeout; halting");
            disruptor.halt();
        }
    }

    private static void validatePowerOfTwo(int ringBufferSize) {
        if (ringBufferSize <= 0 || Integer.bitCount(ringBufferSize) != 1) {
            throw new IllegalArgumentException("Ring buffer size must be a positive power of two");
        }
    }

    public enum WaitMode {
        YIELDING,
        BUSY_SPIN
    }
}
