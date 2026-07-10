package exchange.journal;

import Price.Price;
import Price.PriceFactory;
import exchange.core.AffinityThreadFactory;
import exchange.model.AdminCommand;
import exchange.model.AdminOperation;
import exchange.model.CommandType;
import exchange.model.MutableOrderCommand;
import exchange.model.NewOrderCommand;
import exchange.model.OrderCommand;
import exchange.model.OrderType;
import exchange.model.SelfTradePreventionMode;
import exchange.model.Side;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.bytes.SyncMode;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Wire;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public final class ChronicleCommandJournal implements CommandJournal {
    private static final Logger logger = LoggerFactory.getLogger(ChronicleCommandJournal.class);
    private static final int DEFAULT_RING_SIZE = 65_536;
    private static final long UNPUBLISHED = -1L;
    private static final long PRICE_NOT_SET = Long.MIN_VALUE;
    private static final int DEFAULT_IDLE_SPINS = 1_024;
    private static final int DEFAULT_IDLE_YIELDS = 64;
    private static final long DEFAULT_IDLE_PARK_NANOS = 1_000L;
    private static final long DEFAULT_ERROR_PARK_NANOS = 100_000L;

    private final JournalSlot[] ring;
    private final int mask;
    private final ChronicleQueue queue;
    private final ExcerptAppender appender;
    private final int idleSpins;
    private final int idleYields;
    private final long idleParkNanos;
    private final long errorParkNanos;
    private final AtomicLong nextWriteSequence = new AtomicLong();
    private final AtomicLong persistedSequence = new AtomicLong();
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final Thread writerThread;

    public ChronicleCommandJournal(Path path) {
        this(path, DEFAULT_RING_SIZE);
    }

    public ChronicleCommandJournal(Path path, int ringSize) {
        if (Integer.bitCount(ringSize) != 1) {
            throw new IllegalArgumentException("Chronicle journal ring size must be a power of two");
        }
        try {
            Files.createDirectories(path);
        } catch (Exception e) {
            throw new IllegalStateException("Unable to create Chronicle journal directory: " + path, e);
        }
        this.ring = new JournalSlot[ringSize];
        for (int i = 0; i < ring.length; i++) {
            ring[i] = new JournalSlot();
        }
        this.mask = ringSize - 1;
        this.idleSpins = intSetting("chronicleJournalIdleSpins", "CHRONICLE_JOURNAL_IDLE_SPINS",
                DEFAULT_IDLE_SPINS);
        this.idleYields = intSetting("chronicleJournalIdleYields", "CHRONICLE_JOURNAL_IDLE_YIELDS",
                DEFAULT_IDLE_YIELDS);
        this.idleParkNanos = longSetting("chronicleJournalIdleParkNanos", "CHRONICLE_JOURNAL_IDLE_PARK_NANOS",
                DEFAULT_IDLE_PARK_NANOS);
        this.errorParkNanos = longSetting("chronicleJournalErrorParkNanos", "CHRONICLE_JOURNAL_ERROR_PARK_NANOS",
                DEFAULT_ERROR_PARK_NANOS);
        this.queue = buildQueue(path);
        this.appender = queue.createAppender();
        ThreadFactory threadFactory = new AffinityThreadFactory("chronicle-command-journal");
        this.writerThread = threadFactory.newThread(this::writerLoop);
        this.writerThread.setDaemon(true);
        this.writerThread.setPriority(intSetting("chronicleJournalThreadPriority",
                "CHRONICLE_JOURNAL_THREAD_PRIORITY", Thread.NORM_PRIORITY));
        this.writerThread.start();
    }

    @Override
    public void append(OrderCommand command) {
        long writeSequence = nextWriteSequence.getAndIncrement();
        long oldestRetained = writeSequence - ring.length;
        if (persistedSequence.get() <= oldestRetained) {
            throw new IllegalStateException("Chronicle command journal ring is full; persistence is behind ingress");
        }
        ring[index(writeSequence)].copyFrom(command, writeSequence);
    }

    @Override
    public List<OrderCommand> replay() {
        ArrayList<OrderCommand> commands = new ArrayList<>();
        ExcerptTailer tailer = queue.createTailer();
        tailer.toStart();
        while (true) {
            try (DocumentContext document = tailer.readingDocument()) {
                if (!document.isPresent()) {
                    break;
                }
                OrderCommand command = readCommand(document.wire());
                if (command != null) {
                    commands.add(command);
                }
            }
        }
        return List.copyOf(commands);
    }

    @Override
    public long size() {
        return Math.min(totalAppended(), ring.length);
    }

    @Override
    public long totalAppended() {
        return nextWriteSequence.get();
    }

    @Override
    public void close() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        try {
            writerThread.join(5_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        queue.close();
    }

    private void writerLoop() {
        long nextToPersist = persistedSequence.get();
        int idleCount = 0;
        while (running.get() || nextToPersist < nextWriteSequence.get()) {
            JournalSlot slot = ring[index(nextToPersist)];
            if (!slot.isPublished(nextToPersist)) {
                idleCount = idle(idleCount);
                continue;
            }
            try {
                writeSlot(slot);
                slot.clear(nextToPersist);
                nextToPersist++;
                persistedSequence.set(nextToPersist);
                idleCount = 0;
            } catch (RuntimeException e) {
                logger.error("Chronicle command journal writer failed at ring sequence {}", nextToPersist, e);
                LockSupport.parkNanos(errorParkNanos);
            }
        }
    }

    private ChronicleQueue buildQueue(Path path) {
        SingleChronicleQueueBuilder builder = SingleChronicleQueueBuilder.binary(path)
                .rollCycle(RollCycles.FAST_DAILY);

        long blockSize = longSetting("chronicleJournalBlockSize", "CHRONICLE_JOURNAL_BLOCK_SIZE", 0L);
        if (blockSize > 0) {
            builder.blockSize(blockSize);
        }

        long preloaderIntervalMillis = longSetting("chronicleJournalPreloaderIntervalMillis",
                "CHRONICLE_JOURNAL_PRELOADER_INTERVAL_MS", 0L);
        if (preloaderIntervalMillis > 0) {
            if (SingleChronicleQueueBuilder.areEnterpriseFeaturesAvailable()) {
                builder.enablePreloader(preloaderIntervalMillis);
            } else {
                logger.warn("Ignoring CHRONICLE_JOURNAL_PRELOADER_INTERVAL_MS because Chronicle Queue Enterprise "
                        + "is not on the classpath");
            }
        }

        String syncMode = stringSetting("chronicleJournalSyncMode", "CHRONICLE_JOURNAL_SYNC_MODE", "");
        if (!syncMode.isBlank()) {
            builder.syncMode(SyncMode.valueOf(syncMode.trim().toUpperCase()));
        }

        return builder.build();
    }

    private int idle(int idleCount) {
        int nextIdleCount = idleCount + 1;
        if (nextIdleCount <= idleSpins) {
            Thread.onSpinWait();
        } else if (nextIdleCount <= idleSpins + idleYields) {
            Thread.yield();
        } else if (idleParkNanos > 0) {
            LockSupport.parkNanos(idleParkNanos);
        }
        return nextIdleCount;
    }

    private void writeSlot(JournalSlot slot) {
        try (DocumentContext document = appender.writingDocument()) {
            Wire wire = document.wire();
            wire.write("commandType").text(slot.commandType == null ? null : slot.commandType.name());
            wire.write("sequenceNumber").int64(slot.sequenceNumber);
            wire.write("epochSecond").int64(slot.inboundEpochSecond);
            wire.write("nano").int32(slot.inboundNano);
            wire.write("orderId").text(slot.orderId);
            wire.write("clientId").text(slot.clientId);
            wire.write("symbol").text(slot.symbol);
            wire.write("side").text(slot.side == null ? null : slot.side.name());
            wire.write("orderType").text(slot.orderType == null ? null : slot.orderType.name());
            wire.write("priceCents").int64(slot.priceCents);
            wire.write("quantity").int32(slot.quantity);
            wire.write("stpMode").text(slot.stpMode == null ? null : slot.stpMode.name());
            wire.write("adminOperation").text(slot.adminOperation == null ? null : slot.adminOperation.name());
        }
    }

    private OrderCommand readCommand(Wire wire) {
        CommandType commandType = parseEnum(CommandType.class, wire.read("commandType").text());
        long sequenceNumber = wire.read("sequenceNumber").int64();
        long epochSecond = wire.read("epochSecond").int64();
        int nano = wire.read("nano").int32();
        String orderId = wire.read("orderId").text();
        String clientId = wire.read("clientId").text();
        String symbol = wire.read("symbol").text();
        if (commandType == CommandType.NEW_ORDER) {
            Side side = parseEnum(Side.class, wire.read("side").text());
            OrderType orderType = parseEnum(OrderType.class, wire.read("orderType").text());
            long priceCents = wire.read("priceCents").int64();
            int quantity = wire.read("quantity").int32();
            SelfTradePreventionMode stpMode = parseEnum(SelfTradePreventionMode.class, wire.read("stpMode").text());
            Price price = priceCents == PRICE_NOT_SET ? null : makePrice(priceCents);
            return new NewOrderCommand(sequenceNumber, Instant.ofEpochSecond(epochSecond, nano), orderId, clientId,
                    symbol, side, orderType, price, quantity, stpMode);
        }
        if (commandType == CommandType.ADMIN) {
            AdminOperation operation = parseEnum(AdminOperation.class, wire.read("adminOperation").text());
            return new AdminCommand(sequenceNumber, Instant.ofEpochSecond(epochSecond, nano), orderId, clientId,
                    symbol, operation);
        }
        return new JournalSnapshot(commandType, sequenceNumber, Instant.ofEpochSecond(epochSecond, nano), orderId,
                clientId, symbol, 0);
    }

    private int index(long sequence) {
        return (int) sequence & mask;
    }

    private Price makePrice(long cents) {
        try {
            return PriceFactory.makePrice(cents);
        } catch (Exception e) {
            throw new IllegalStateException("Chronicle command journal contains an invalid price: " + cents, e);
        }
    }

    private static <T extends Enum<T>> T parseEnum(Class<T> type, String value) {
        return value == null || value.isBlank() ? null : Enum.valueOf(type, value);
    }

    private static int intSetting(String propertyName, String envName, int defaultValue) {
        String configured = stringSetting(propertyName, envName, "");
        if (configured.isBlank()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(configured.trim());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private static long longSetting(String propertyName, String envName, long defaultValue) {
        String configured = stringSetting(propertyName, envName, "");
        if (configured.isBlank()) {
            return defaultValue;
        }
        try {
            return Long.parseLong(configured.trim());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private static String stringSetting(String propertyName, String envName, String defaultValue) {
        return System.getProperty(propertyName, System.getenv().getOrDefault(envName, defaultValue));
    }

    private static final class JournalSlot {
        private volatile long publishedSequence = UNPUBLISHED;
        private CommandType commandType;
        private long sequenceNumber;
        private long inboundEpochSecond;
        private int inboundNano;
        private String orderId;
        private String clientId;
        private String symbol;
        private Side side;
        private OrderType orderType;
        private long priceCents = PRICE_NOT_SET;
        private int quantity;
        private SelfTradePreventionMode stpMode;
        private AdminOperation adminOperation;

        private void copyFrom(OrderCommand command, long writeSequence) {
            publishedSequence = UNPUBLISHED;
            commandType = command.commandType();
            sequenceNumber = command.sequenceNumber();
            Instant inboundTimestamp = command.inboundTimestamp();
            inboundEpochSecond = inboundTimestamp.getEpochSecond();
            inboundNano = inboundTimestamp.getNano();
            orderId = command.orderId();
            clientId = command.clientId();
            symbol = command.symbol();
            side = null;
            orderType = null;
            priceCents = PRICE_NOT_SET;
            quantity = 0;
            stpMode = null;
            adminOperation = null;
            if (command instanceof NewOrderCommand newOrder) {
                copyNewOrder(newOrder.side(), newOrder.orderType(), newOrder.price(), newOrder.quantity(),
                        newOrder.stpMode());
            } else if (command instanceof MutableOrderCommand mutable) {
                copyNewOrder(mutable.side(), mutable.orderType(), mutable.price(), mutable.quantity(),
                        mutable.stpMode());
            } else if (command instanceof AdminCommand admin) {
                adminOperation = admin.operation();
            }
            publishedSequence = writeSequence;
        }

        private void copyNewOrder(Side side, OrderType orderType, Price price, int quantity,
                SelfTradePreventionMode stpMode) {
            this.side = side;
            this.orderType = orderType;
            this.priceCents = price == null ? PRICE_NOT_SET : price.getCents();
            this.quantity = quantity;
            this.stpMode = stpMode;
        }

        private boolean isPublished(long expectedSequence) {
            return publishedSequence == expectedSequence;
        }

        private void clear(long expectedSequence) {
            if (publishedSequence == expectedSequence) {
                publishedSequence = UNPUBLISHED;
            }
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
