package exchange.core;

import com.lmax.disruptor.ExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Fail-safe Disruptor exception boundary for runtime rings.
 *
 * <p>The event exception path avoids stack formatting and message construction.
 * It increments counters and invokes a pre-wired recovery callback so the
 * processor can advance to the next sequence without halting the ring.</p>
 */
public final class FailSafeDisruptorExceptionHandler<T> implements ExceptionHandler<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(FailSafeDisruptorExceptionHandler.class);
    private static final EventRecovery<?> NOOP_RECOVERY = (event, sequence, error) -> {
    };

    private final String ringName;
    private final EventRecovery<T> recovery;
    private final AtomicLong eventFailures = new AtomicLong();
    private final AtomicLong startFailures = new AtomicLong();
    private final AtomicLong shutdownFailures = new AtomicLong();

    public FailSafeDisruptorExceptionHandler(String ringName) {
        this(ringName, noopRecovery());
    }

    public FailSafeDisruptorExceptionHandler(String ringName, EventRecovery<T> recovery) {
        this.ringName = Objects.requireNonNull(ringName, "ringName");
        this.recovery = Objects.requireNonNull(recovery, "recovery");
    }

    @Override
    public void handleEventException(Throwable ex, long sequence, T event) {
        eventFailures.incrementAndGet();
        try {
            recovery.recover(event, sequence, ex);
        } catch (Throwable recoveryFailure) {
            eventFailures.incrementAndGet();
        }
    }

    @Override
    public void handleOnStartException(Throwable ex) {
        startFailures.incrementAndGet();
        LOGGER.error("Disruptor start failure on {}", ringName, ex);
    }

    @Override
    public void handleOnShutdownException(Throwable ex) {
        shutdownFailures.incrementAndGet();
        LOGGER.error("Disruptor shutdown failure on {}", ringName, ex);
    }

    public long eventFailures() {
        return eventFailures.get();
    }

    public long startFailures() {
        return startFailures.get();
    }

    public long shutdownFailures() {
        return shutdownFailures.get();
    }

    @SuppressWarnings("unchecked")
    private static <T> EventRecovery<T> noopRecovery() {
        return (EventRecovery<T>) NOOP_RECOVERY;
    }

    @FunctionalInterface
    public interface EventRecovery<T> {
        void recover(T event, long sequence, Throwable error);
    }
}
