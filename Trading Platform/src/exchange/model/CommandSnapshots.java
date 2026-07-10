package exchange.model;

/**
 * Creates immutable command copies before commands cross journal, replication,
 * or asynchronous shard boundaries.
 */
public final class CommandSnapshots {
    private CommandSnapshots() {
    }

    public static OrderCommand immutable(OrderCommand command) {
        if (command instanceof NewOrderCommand newOrder) {
            return newOrder;
        }
        if (command instanceof AdminCommand admin) {
            return admin;
        }
        if (command instanceof MutableAdminCommand admin) {
            return new AdminCommand(admin.sequenceNumber(), admin.inboundTimestamp(), admin.orderId(),
                    admin.clientId(), admin.symbol(), admin.symbolId(), admin.operation());
        }
        if (command instanceof CancelOrderCommand cancel) {
            return cancel;
        }
        if (command instanceof MutableCancelOrderCommand cancel) {
            return new CancelOrderCommand(cancel.sequenceNumber(), cancel.inboundTimestamp(), cancel.orderId(),
                    cancel.clientId(), cancel.symbol(), cancel.symbolId());
        }
        if (command instanceof ModifyOrderCommand modify) {
            return modify;
        }
        if (command instanceof MutableModifyOrderCommand modify) {
            return new ModifyOrderCommand(modify.sequenceNumber(), modify.inboundTimestamp(), modify.orderId(),
                    modify.clientId(), modify.symbol(), modify.symbolId(), modify.newPrice(), modify.newQuantity());
        }
        if (command instanceof MutableOrderCommand mutable) {
            return new NewOrderCommand(
                    mutable.sequenceNumber(),
                    mutable.inboundTimestamp(),
                    mutable.orderId(),
                    mutable.clientId(),
                    mutable.symbol(),
                    mutable.symbolId(),
                    mutable.side(),
                    mutable.orderType(),
                    mutable.price(),
                    mutable.quantity(),
                    mutable.stpMode(),
                    mutable.ingressTimeNs());
        }
        return new BasicCommandSnapshot(command.commandType(), command.sequenceNumber(), command.inboundTimestamp(),
                command.orderId(), command.clientId(), command.symbol(), command.symbolId(), command.ingressTimeNs());
    }

    private record BasicCommandSnapshot(
            CommandType commandType,
            long sequenceNumber,
            java.time.Instant inboundTimestamp,
            String orderId,
            String clientId,
            String symbol,
            int symbolId,
            long ingressTimeNs) implements OrderCommand {

        @Override
        public OrderCommand withSequencing(long sequenceNumber, java.time.Instant inboundTimestamp) {
            return new BasicCommandSnapshot(commandType, sequenceNumber, inboundTimestamp, orderId, clientId, symbol,
                    symbolId, ingressTimeNs);
        }
    }
}
