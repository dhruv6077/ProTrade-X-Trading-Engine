package exchange.replication;

import exchange.model.OrderCommand;

public record ReplicationRecord(OrderCommand command, boolean acceptedForMatching) {
    public long sequenceNumber() {
        return command.sequenceNumber();
    }
}
