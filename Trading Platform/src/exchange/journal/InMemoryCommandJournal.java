package exchange.journal;

import exchange.model.OrderCommand;
import exchange.model.CommandType;
import exchange.model.AdminCommand;
import exchange.model.AdminOperation;

import java.util.ArrayList;
import java.util.List;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

public final class InMemoryCommandJournal implements CommandJournal {
    private static final int DEFAULT_CAPACITY = 1_000_000;
    private static final long UNPUBLISHED = -1L;

    private final JournalEntry[] commands;
    private final AtomicLong nextWriteSequence = new AtomicLong();

    public InMemoryCommandJournal() {
        this(DEFAULT_CAPACITY);
    }

    public InMemoryCommandJournal(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("Journal capacity must be positive");
        }
        this.commands = new JournalEntry[capacity];
        for (int i = 0; i < capacity; i++) {
            this.commands[i] = new JournalEntry();
        }
    }

    @Override
    public void append(OrderCommand command) {
        long writeSequence = nextWriteSequence.getAndIncrement();
        commands[index(writeSequence)].copyFrom(command, writeSequence);
    }

    @Override
    public List<OrderCommand> replay() {
        long endExclusive = nextWriteSequence.get();
        long retained = Math.min(endExclusive, commands.length);
        long startInclusive = endExclusive - retained;
        ArrayList<OrderCommand> snapshot = new ArrayList<>((int) retained);
        for (long sequence = startInclusive; sequence < endExclusive; sequence++) {
            JournalEntry command = commands[index(sequence)];
            OrderCommand commandSnapshot = command.snapshotIfPublished(sequence);
            if (commandSnapshot != null) {
                snapshot.add(commandSnapshot);
            }
        }
        return List.copyOf(snapshot);
    }

    @Override
    public long size() {
        return Math.min(nextWriteSequence.get(), commands.length);
    }

    public long totalAppended() {
        return nextWriteSequence.get();
    }

    @Override
    public void close() {
        for (JournalEntry command : commands) {
            if (command != null) {
                command.reset();
            }
        }
        nextWriteSequence.set(0);
    }

    private int index(long sequence) {
        return (int) (sequence % commands.length);
    }

    private static final class JournalEntry implements OrderCommand {
        private volatile long publishedSequence = UNPUBLISHED;
        private CommandType commandType;
        private long sequenceNumber;
        private Instant inboundTimestamp = Instant.EPOCH;
        private String orderId;
        private String clientId;
        private String symbol;
        private int symbolId;
        private AdminOperation adminOperation;

        private void copyFrom(OrderCommand command, long writeSequence) {
            publishedSequence = UNPUBLISHED;
            commandType = command.commandType();
            sequenceNumber = command.sequenceNumber();
            inboundTimestamp = command.inboundTimestamp();
            orderId = command.orderId();
            clientId = command.clientId();
            symbol = command.symbol();
            symbolId = command.symbolId();
            adminOperation = command instanceof AdminCommand admin ? admin.operation() : null;
            publishedSequence = writeSequence;
        }

        private OrderCommand snapshotIfPublished(long expectedSequence) {
            if (publishedSequence != expectedSequence) {
                return null;
            }
            CommandType snapshotCommandType = commandType;
            long snapshotSequenceNumber = sequenceNumber;
            Instant snapshotInboundTimestamp = inboundTimestamp;
            String snapshotOrderId = orderId;
            String snapshotClientId = clientId;
            String snapshotSymbol = symbol;
            int snapshotSymbolId = symbolId;
            AdminOperation snapshotAdminOperation = adminOperation;
            if (publishedSequence != expectedSequence) {
                return null;
            }
            if (snapshotCommandType == CommandType.ADMIN) {
                return new AdminCommand(snapshotSequenceNumber, snapshotInboundTimestamp, snapshotOrderId,
                        snapshotClientId, snapshotSymbol, snapshotSymbolId, snapshotAdminOperation);
            }
            return new JournalSnapshot(snapshotCommandType, snapshotSequenceNumber, snapshotInboundTimestamp,
                    snapshotOrderId, snapshotClientId, snapshotSymbol, snapshotSymbolId);
        }

        private void reset() {
            publishedSequence = UNPUBLISHED;
            commandType = null;
            sequenceNumber = 0;
            inboundTimestamp = Instant.EPOCH;
            orderId = null;
            clientId = null;
            symbol = null;
            symbolId = 0;
            adminOperation = null;
        }

        @Override
        public CommandType commandType() {
            return commandType;
        }

        @Override
        public long sequenceNumber() {
            return sequenceNumber;
        }

        @Override
        public Instant inboundTimestamp() {
            return inboundTimestamp;
        }

        @Override
        public String clientId() {
            return clientId;
        }

        @Override
        public String symbol() {
            return symbol;
        }

        @Override
        public int symbolId() {
            return symbolId;
        }

        @Override
        public String orderId() {
            return orderId;
        }

        @Override
        public OrderCommand withSequencing(long sequenceNumber, Instant inboundTimestamp) {
            this.sequenceNumber = sequenceNumber;
            this.inboundTimestamp = inboundTimestamp;
            return this;
        }
    }

    private record JournalSnapshot(
            CommandType commandType,
            long sequenceNumber,
            Instant inboundTimestamp,
            String orderId,
            String clientId,
            String symbol,
            int symbolId) implements OrderCommand {

        @Override
        public OrderCommand withSequencing(long sequenceNumber, Instant inboundTimestamp) {
            return new JournalSnapshot(commandType, sequenceNumber, inboundTimestamp, orderId, clientId, symbol,
                    symbolId);
        }
    }
}
