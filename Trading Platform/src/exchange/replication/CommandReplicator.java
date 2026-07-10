package exchange.replication;

import exchange.model.OrderCommand;

public interface CommandReplicator extends AutoCloseable {
    ReplicationAck replicate(OrderCommand command, boolean acceptedForMatching);

    long replicatedThrough();

    default boolean caughtUpTo(long sequenceNumber) {
        return replicatedThrough() >= sequenceNumber;
    }

    @Override
    default void close() {
    }
}
