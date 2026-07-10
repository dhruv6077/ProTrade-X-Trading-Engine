package exchange.journal;

import exchange.model.CommandType;
import exchange.model.OrderCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.nio.charset.StandardCharsets;

public final class AsyncDiskCommandJournal implements CommandJournal {
    private static final Logger logger = LoggerFactory.getLogger(AsyncDiskCommandJournal.class);
    private static final int DEFAULT_CAPACITY = 1_000_000;
    private static final long UNPUBLISHED = -1L;

    private final JournalEntry[] commands;
    private final AtomicLong nextWriteSequence = new AtomicLong();

    private final FileChannel fileChannel;
    private final Thread writerThread;
    private volatile boolean running = true;

    public AsyncDiskCommandJournal(Path journalPath) throws IOException {
        this(DEFAULT_CAPACITY, journalPath);
    }

    public AsyncDiskCommandJournal(int capacity, Path journalPath) throws IOException {
        if (capacity <= 0) {
            throw new IllegalArgumentException("Journal capacity must be positive");
        }
        this.commands = new JournalEntry[capacity];
        for (int i = 0; i < capacity; i++) {
            this.commands[i] = new JournalEntry();
        }

        this.fileChannel = FileChannel.open(journalPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
        this.writerThread = new Thread(this::writerLoop, "disk-journal-writer");
        this.writerThread.setDaemon(true);
        this.writerThread.start();
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
        running = false;
        try {
            writerThread.join(2000);
            fileChannel.close();
        } catch (Exception e) {
            logger.error("Failed to close AsyncDiskCommandJournal cleanly", e);
        }
        for (JournalEntry command : commands) {
            if (command != null) {
                command.reset();
            }
        }
        nextWriteSequence.set(0);
    }

    private void writerLoop() {
        long diskSequence = 0;
        ByteBuffer buffer = ByteBuffer.allocateDirect(1024);

        while (running || diskSequence < nextWriteSequence.get()) {
            JournalEntry entry = commands[index(diskSequence)];
            OrderCommand cmd = entry.snapshotIfPublished(diskSequence);

            if (cmd != null) {
                try {
                    buffer.clear();
                    buffer.putInt(cmd.commandType() != null ? cmd.commandType().ordinal() : -1);
                    buffer.putLong(cmd.sequenceNumber());
                    buffer.putLong(cmd.inboundTimestamp().getEpochSecond());
                    buffer.putInt(cmd.inboundTimestamp().getNano());
                    writeString(buffer, cmd.orderId());
                    writeString(buffer, cmd.clientId());
                    writeString(buffer, cmd.symbol());
                    buffer.flip();

                    while (buffer.hasRemaining()) {
                        fileChannel.write(buffer);
                    }
                    diskSequence++;
                } catch (IOException e) {
                    logger.error("Failed to write to disk journal", e);
                }
            } else {
                Thread.yield();
            }
        }
    }

    private void writeString(ByteBuffer buffer, String str) {
        if (str == null) {
            buffer.putInt(-1);
        } else {
            byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
            buffer.putInt(bytes.length);
            buffer.put(bytes);
        }
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

        private void copyFrom(OrderCommand command, long writeSequence) {
            publishedSequence = UNPUBLISHED;
            commandType = command.commandType();
            sequenceNumber = command.sequenceNumber();
            inboundTimestamp = command.inboundTimestamp();
            orderId = command.orderId();
            clientId = command.clientId();
            symbol = command.symbol();
            symbolId = command.symbolId();
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
            if (publishedSequence != expectedSequence) {
                return null;
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
