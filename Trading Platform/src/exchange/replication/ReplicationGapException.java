package exchange.replication;

public final class ReplicationGapException extends IllegalStateException {
    public ReplicationGapException(long expectedSequence, long actualSequence) {
        super("Replication stream gap: expected sequence " + expectedSequence + " but received " + actualSequence);
    }
}
