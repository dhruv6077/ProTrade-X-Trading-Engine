package exchange.core;

import exchange.model.OrderCommand;

import java.time.Clock;
import java.util.concurrent.atomic.AtomicLong;

public final class Sequencer {
    private final AtomicLong nextSequence;
    private final Clock clock;

    public Sequencer() {
        this(1, Clock.systemUTC());
    }

    public Sequencer(long firstSequenceNumber) {
        this(firstSequenceNumber, Clock.systemUTC());
    }

    public Sequencer(long firstSequenceNumber, Clock clock) {
        this.nextSequence = new AtomicLong(firstSequenceNumber);
        this.clock = clock;
    }

    public OrderCommand sequence(OrderCommand command) {
        return command.withSequencing(nextSequence.getAndIncrement(), clock.instant());
    }

    public void advanceToAtLeast(long nextSequenceNumber) {
        nextSequence.updateAndGet(current -> Math.max(current, nextSequenceNumber));
    }

    public long nextSequenceNumber() {
        return nextSequence.get();
    }
}
