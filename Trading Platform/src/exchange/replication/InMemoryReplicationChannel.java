package exchange.replication;

import Price.Price;
import exchange.model.AdminCommand;
import exchange.model.AdminOperation;
import exchange.model.CancelOrderCommand;
import exchange.model.CommandType;
import exchange.model.ModifyOrderCommand;
import exchange.model.MutableAdminCommand;
import exchange.model.MutableModifyOrderCommand;
import exchange.model.MutableOrderCommand;
import exchange.model.NewOrderCommand;
import exchange.model.OrderCommand;
import exchange.model.OrderType;
import exchange.model.SelfTradePreventionMode;
import exchange.model.Side;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Bounded active-passive replication channel for local HA tests and development.
 *
 * <p>The production transport can replace this with Aeron or a custom UDP/TCP
 * stream while preserving the same sequenced command contract.</p>
 */
public final class InMemoryReplicationChannel implements CommandReplicator {
    private static final long UNPUBLISHED = -1L;

    private final Slot[] slots;
    private final AtomicLong nextWriteSequence = new AtomicLong();
    private final AtomicLong replicatedThrough = new AtomicLong();

    public InMemoryReplicationChannel() {
        this(65_536);
    }

    public InMemoryReplicationChannel(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("Replication channel capacity must be positive");
        }
        slots = new Slot[capacity];
        for (int i = 0; i < capacity; i++) {
            slots[i] = new Slot();
        }
    }

    @Override
    public ReplicationAck replicate(OrderCommand command, boolean acceptedForMatching) {
        if (command.sequenceNumber() <= 0L) {
            throw new IllegalArgumentException("Only sequenced commands can be replicated");
        }
        long writeSequence = nextWriteSequence.getAndIncrement();
        slots[index(writeSequence)].copyFrom(command, acceptedForMatching, writeSequence);
        replicatedThrough.accumulateAndGet(command.sequenceNumber(), Math::max);
        return new ReplicationAck(command.sequenceNumber(), acceptedForMatching);
    }

    public List<ReplicationRecord> replay() {
        long endExclusive = nextWriteSequence.get();
        long retained = Math.min(endExclusive, slots.length);
        long startInclusive = endExclusive - retained;
        ArrayList<ReplicationRecord> records = new ArrayList<>((int) retained);
        for (long sequence = startInclusive; sequence < endExclusive; sequence++) {
            ReplicationRecord record = slots[index(sequence)].snapshotIfPublished(sequence);
            if (record != null) {
                records.add(record);
            }
        }
        return List.copyOf(records);
    }

    public long size() {
        return Math.min(nextWriteSequence.get(), slots.length);
    }

    public long totalReplicated() {
        return nextWriteSequence.get();
    }

    @Override
    public long replicatedThrough() {
        return replicatedThrough.get();
    }

    @Override
    public void close() {
        for (Slot slot : slots) {
            slot.reset();
        }
        nextWriteSequence.set(0L);
        replicatedThrough.set(0L);
    }

    private int index(long writeSequence) {
        return (int) (writeSequence % slots.length);
    }

    private static final class Slot {
        private volatile long publishedSequence = UNPUBLISHED;
        private boolean acceptedForMatching;
        private CommandType commandType;
        private long sequenceNumber;
        private Instant inboundTimestamp = Instant.EPOCH;
        private String orderId;
        private String clientId;
        private String symbol;
        private int symbolId;
        private Side side;
        private OrderType orderType;
        private Price price;
        private int quantity;
        private SelfTradePreventionMode stpMode;
        private long ingressTimeNs;
        private AdminOperation adminOperation;
        private Price newPrice;
        private int newQuantity;

        private void copyFrom(OrderCommand source, boolean acceptedForMatching, long writeSequence) {
            publishedSequence = UNPUBLISHED;
            this.acceptedForMatching = acceptedForMatching;
            commandType = source.commandType();
            sequenceNumber = source.sequenceNumber();
            inboundTimestamp = source.inboundTimestamp();
            orderId = source.orderId();
            clientId = source.clientId();
            symbol = source.symbol();
            symbolId = source.symbolId();
            ingressTimeNs = source.ingressTimeNs();
            side = null;
            orderType = null;
            price = null;
            quantity = 0;
            stpMode = null;
            adminOperation = null;
            newPrice = null;
            newQuantity = 0;

            if (source instanceof NewOrderCommand newOrder) {
                side = newOrder.side();
                orderType = newOrder.orderType();
                price = newOrder.price();
                quantity = newOrder.quantity();
                stpMode = newOrder.stpMode();
            } else if (source instanceof MutableOrderCommand mutable) {
                side = mutable.side();
                orderType = mutable.orderType();
                price = mutable.price();
                quantity = mutable.quantity();
                stpMode = mutable.stpMode();
            } else if (source instanceof AdminCommand admin) {
                adminOperation = admin.operation();
            } else if (source instanceof MutableAdminCommand admin) {
                adminOperation = admin.operation();
            } else if (source instanceof ModifyOrderCommand modify) {
                newPrice = modify.newPrice();
                newQuantity = modify.newQuantity();
            } else if (source instanceof MutableModifyOrderCommand modify) {
                newPrice = modify.newPrice();
                newQuantity = modify.newQuantity();
            }
            publishedSequence = writeSequence;
        }

        private ReplicationRecord snapshotIfPublished(long expectedSequence) {
            if (publishedSequence != expectedSequence) {
                return null;
            }
            boolean snapshotAccepted = acceptedForMatching;
            OrderCommand snapshotCommand = snapshotCommand();
            if (publishedSequence != expectedSequence) {
                return null;
            }
            return new ReplicationRecord(snapshotCommand, snapshotAccepted);
        }

        private OrderCommand snapshotCommand() {
            return switch (commandType) {
                case NEW_ORDER -> new NewOrderCommand(sequenceNumber, inboundTimestamp, orderId, clientId, symbol,
                        symbolId, side, orderType, price, quantity, stpMode, ingressTimeNs);
                case CANCEL_ORDER -> new CancelOrderCommand(sequenceNumber, inboundTimestamp, orderId, clientId,
                        symbol, symbolId);
                case MODIFY_ORDER -> new ModifyOrderCommand(sequenceNumber, inboundTimestamp, orderId, clientId,
                        symbol, symbolId, newPrice, newQuantity);
                case ADMIN -> new AdminCommand(sequenceNumber, inboundTimestamp, orderId, clientId, symbol, symbolId,
                        adminOperation);
            };
        }

        private void reset() {
            publishedSequence = UNPUBLISHED;
            acceptedForMatching = false;
            commandType = null;
            sequenceNumber = 0L;
            inboundTimestamp = Instant.EPOCH;
            orderId = null;
            clientId = null;
            symbol = null;
            symbolId = 0;
            side = null;
            orderType = null;
            price = null;
            quantity = 0;
            stpMode = null;
            ingressTimeNs = 0L;
            adminOperation = null;
            newPrice = null;
            newQuantity = 0;
        }
    }
}
