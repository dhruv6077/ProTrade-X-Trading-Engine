package exchange.replication;

public record ReplicationAck(long sequenceNumber, boolean acceptedForMatching) {
}
