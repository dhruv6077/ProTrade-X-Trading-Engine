package exchange.replication;

import exchange.model.OrderCommand;

public final class NoOpCommandReplicator implements CommandReplicator {
    public static final NoOpCommandReplicator INSTANCE = new NoOpCommandReplicator();

    private NoOpCommandReplicator() {
    }

    @Override
    public ReplicationAck replicate(OrderCommand command, boolean acceptedForMatching) {
        return new ReplicationAck(command.sequenceNumber(), acceptedForMatching);
    }

    @Override
    public long replicatedThrough() {
        return Long.MAX_VALUE;
    }
}
