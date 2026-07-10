package exchange.model;

import java.time.Instant;

public interface OrderCommand {
    CommandType commandType();

    long sequenceNumber();

    Instant inboundTimestamp();

    String clientId();

    String symbol();

    default int symbolId() {
        return 0;
    }

    String orderId();

    default long ingressTimeNs() {
        return 0L;
    }

    OrderCommand withSequencing(long sequenceNumber, Instant inboundTimestamp);
}
