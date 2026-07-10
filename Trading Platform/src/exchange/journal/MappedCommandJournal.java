package exchange.journal;

import Price.Price;
import Price.PriceFactory;
import exchange.model.AdminCommand;
import exchange.model.AdminOperation;
import exchange.model.CommandType;
import exchange.model.MutableOrderCommand;
import exchange.model.NewOrderCommand;
import exchange.model.OrderCommand;
import exchange.model.OrderType;
import exchange.model.SelfTradePreventionMode;
import exchange.model.Side;

import java.io.IOException;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Deterministic write-ahead command journal backed by a memory-mapped file.
 *
 * <p>The hot append path uses absolute writes into fixed-size binary records.
 * The sequenced command is forced to disk before append returns, so the gateway
 * can safely hand the command to the matching engine only after the recovery
 * log contains the command.</p>
 */
public final class MappedCommandJournal implements CommandJournal {
    private static final int FILE_MAGIC = 0x56544A31; // VTJ1
    private static final int FILE_VERSION = 1;
    private static final int HEADER_SIZE = 64;
    private static final int RECORD_SIZE = 256;

    private static final int COMMIT_SEQUENCE_OFFSET = 0;
    private static final int RECORD_VERSION_OFFSET = 8;
    private static final int COMMAND_TYPE_OFFSET = 12;
    private static final int SEQUENCE_NUMBER_OFFSET = 16;
    private static final int EPOCH_SECOND_OFFSET = 24;
    private static final int NANO_OFFSET = 32;
    private static final int ORDER_ID_LENGTH_OFFSET = 36;
    private static final int ORDER_ID_OFFSET = 40;
    private static final int ORDER_ID_BYTES = 64;
    private static final int CLIENT_ID_LENGTH_OFFSET = 104;
    private static final int CLIENT_ID_OFFSET = 108;
    private static final int CLIENT_ID_BYTES = 64;
    private static final int SYMBOL_LENGTH_OFFSET = 172;
    private static final int SYMBOL_OFFSET = 176;
    private static final int SYMBOL_BYTES = 32;
    private static final int SIDE_OFFSET = 208;
    private static final int ADMIN_OPERATION_OFFSET = SIDE_OFFSET;
    private static final int ORDER_TYPE_OFFSET = 212;
    private static final int PRICE_CENTS_OFFSET = 216;
    private static final int QUANTITY_OFFSET = 224;
    private static final int STP_MODE_OFFSET = 228;
    private static final int CHECKSUM_OFFSET = 232;

    private static final int ABSENT_ENUM = -1;
    private static final long ABSENT_PRICE = Long.MIN_VALUE;

    private final Path path;
    private final int maxRecords;
    private final long mappedBytes;
    private final FileChannel channel;
    private final MappedByteBuffer mapped;
    private final AtomicLong totalAppended = new AtomicLong();

    public MappedCommandJournal(Path path) {
        this(path, intSetting("mappedJournalMaxRecords", "MAPPED_JOURNAL_MAX_RECORDS", 1_000_000));
    }

    public MappedCommandJournal(Path path, int maxRecords) {
        if (maxRecords <= 0) {
            throw new IllegalArgumentException("Mapped journal maxRecords must be positive");
        }
        try {
            Path parent = path.toAbsolutePath().getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            this.path = path;
            this.maxRecords = maxRecords;
            this.mappedBytes = HEADER_SIZE + ((long) maxRecords * RECORD_SIZE);
            this.channel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.READ,
                    StandardOpenOption.WRITE);
            ensureFileSize(channel, mappedBytes);
            this.mapped = channel.map(FileChannel.MapMode.READ_WRITE, 0, mappedBytes);
            initializeOrValidateHeader();
            this.totalAppended.set(scanCommittedRecords());
        } catch (IOException e) {
            throw new IllegalStateException("Unable to initialize mapped command journal: " + path, e);
        }
    }

    @Override
    public void append(OrderCommand command) {
        long sequenceNumber = command.sequenceNumber();
        if (sequenceNumber <= 0) {
            throw new IllegalArgumentException("Sequenced command must have a positive sequence number");
        }
        if (sequenceNumber > maxRecords) {
            throw new IllegalStateException("Mapped command journal capacity exceeded at sequence " + sequenceNumber);
        }

        int base = recordOffset(sequenceNumber);
        mapped.putLong(base + COMMIT_SEQUENCE_OFFSET, 0L);
        writePayload(base, command);
        mapped.putInt(base + CHECKSUM_OFFSET, checksum(base));

        mapped.force(base + RECORD_VERSION_OFFSET, RECORD_SIZE - RECORD_VERSION_OFFSET);
        VarHandle.releaseFence();
        mapped.putLong(base + COMMIT_SEQUENCE_OFFSET, sequenceNumber);
        mapped.force(base + COMMIT_SEQUENCE_OFFSET, Long.BYTES);
        totalAppended.updateAndGet(current -> Math.max(current, sequenceNumber));
    }

    @Override
    public List<OrderCommand> replay() {
        ArrayList<OrderCommand> commands = new ArrayList<>((int) Math.min(totalAppended.get(), maxRecords));
        for (long sequence = 1; sequence <= maxRecords; sequence++) {
            int base = recordOffset(sequence);
            long committedSequence = mapped.getLong(base + COMMIT_SEQUENCE_OFFSET);
            if (committedSequence == 0L) {
                break;
            }
            if (committedSequence != sequence) {
                throw new IllegalStateException("Mapped journal sequence gap or corruption at expected sequence "
                        + sequence + ", found " + committedSequence);
            }
            int expectedChecksum = mapped.getInt(base + CHECKSUM_OFFSET);
            int actualChecksum = checksum(base);
            if (expectedChecksum != actualChecksum) {
                throw new IllegalStateException("Mapped journal checksum mismatch at sequence " + sequence);
            }
            commands.add(readCommand(base));
        }
        return List.copyOf(commands);
    }

    @Override
    public long size() {
        return totalAppended.get();
    }

    @Override
    public long totalAppended() {
        return totalAppended.get();
    }

    @Override
    public void close() {
        mapped.force();
        try {
            channel.close();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to close mapped command journal: " + path, e);
        }
    }

    private void initializeOrValidateHeader() {
        int magic = mapped.getInt(0);
        if (magic == 0) {
            mapped.putInt(0, FILE_MAGIC);
            mapped.putInt(4, FILE_VERSION);
            mapped.putInt(8, RECORD_SIZE);
            mapped.putInt(12, maxRecords);
            mapped.force(0, HEADER_SIZE);
            return;
        }
        if (magic != FILE_MAGIC) {
            throw new IllegalStateException("Invalid mapped journal magic for " + path);
        }
        int version = mapped.getInt(4);
        int recordSize = mapped.getInt(8);
        int storedMaxRecords = mapped.getInt(12);
        if (version != FILE_VERSION || recordSize != RECORD_SIZE || storedMaxRecords != maxRecords) {
            throw new IllegalStateException("Mapped journal metadata mismatch for " + path);
        }
    }

    private long scanCommittedRecords() {
        long count = 0L;
        for (long sequence = 1; sequence <= maxRecords; sequence++) {
            long committedSequence = mapped.getLong(recordOffset(sequence) + COMMIT_SEQUENCE_OFFSET);
            if (committedSequence == 0L) {
                break;
            }
            if (committedSequence != sequence) {
                throw new IllegalStateException("Mapped journal sequence gap or corruption at expected sequence "
                        + sequence + ", found " + committedSequence);
            }
            count = sequence;
        }
        return count;
    }

    private void writePayload(int base, OrderCommand command) {
        CommandType commandType = command.commandType();
        Instant inboundTimestamp = command.inboundTimestamp();
        mapped.putInt(base + RECORD_VERSION_OFFSET, FILE_VERSION);
        mapped.putInt(base + COMMAND_TYPE_OFFSET, ordinal(commandType));
        mapped.putLong(base + SEQUENCE_NUMBER_OFFSET, command.sequenceNumber());
        mapped.putLong(base + EPOCH_SECOND_OFFSET, inboundTimestamp.getEpochSecond());
        mapped.putInt(base + NANO_OFFSET, inboundTimestamp.getNano());
        writeFixedAscii(base + ORDER_ID_LENGTH_OFFSET, base + ORDER_ID_OFFSET, ORDER_ID_BYTES, command.orderId());
        writeFixedAscii(base + CLIENT_ID_LENGTH_OFFSET, base + CLIENT_ID_OFFSET, CLIENT_ID_BYTES, command.clientId());
        writeFixedAscii(base + SYMBOL_LENGTH_OFFSET, base + SYMBOL_OFFSET, SYMBOL_BYTES, command.symbol());

        Side side = null;
        OrderType orderType = null;
        long priceCents = ABSENT_PRICE;
        int quantity = 0;
        SelfTradePreventionMode stpMode = null;
        AdminOperation adminOperation = null;
        if (command instanceof AdminCommand admin) {
            adminOperation = admin.operation();
        } else if (command instanceof NewOrderCommand newOrder) {
            side = newOrder.side();
            orderType = newOrder.orderType();
            priceCents = priceCents(newOrder.price());
            quantity = newOrder.quantity();
            stpMode = newOrder.stpMode();
        } else if (command instanceof MutableOrderCommand mutable) {
            side = mutable.side();
            orderType = mutable.orderType();
            priceCents = priceCents(mutable.price());
            quantity = mutable.quantity();
            stpMode = mutable.stpMode();
        }
        mapped.putInt(base + SIDE_OFFSET, commandType == CommandType.ADMIN ? ordinal(adminOperation) : ordinal(side));
        mapped.putInt(base + ORDER_TYPE_OFFSET, ordinal(orderType));
        mapped.putLong(base + PRICE_CENTS_OFFSET, priceCents);
        mapped.putInt(base + QUANTITY_OFFSET, quantity);
        mapped.putInt(base + STP_MODE_OFFSET, ordinal(stpMode));
    }

    private OrderCommand readCommand(int base) {
        CommandType commandType = enumValue(CommandType.values(), mapped.getInt(base + COMMAND_TYPE_OFFSET));
        long sequenceNumber = mapped.getLong(base + SEQUENCE_NUMBER_OFFSET);
        long epochSecond = mapped.getLong(base + EPOCH_SECOND_OFFSET);
        int nano = mapped.getInt(base + NANO_OFFSET);
        Instant inboundTimestamp = Instant.ofEpochSecond(epochSecond, nano);
        String orderId = readFixedAscii(base + ORDER_ID_LENGTH_OFFSET, base + ORDER_ID_OFFSET, ORDER_ID_BYTES);
        String clientId = readFixedAscii(base + CLIENT_ID_LENGTH_OFFSET, base + CLIENT_ID_OFFSET, CLIENT_ID_BYTES);
        String symbol = readFixedAscii(base + SYMBOL_LENGTH_OFFSET, base + SYMBOL_OFFSET, SYMBOL_BYTES);

        if (commandType == CommandType.ADMIN) {
            AdminOperation operation = enumValue(AdminOperation.values(), mapped.getInt(base + ADMIN_OPERATION_OFFSET));
            return new AdminCommand(sequenceNumber, inboundTimestamp, orderId, clientId, symbol, operation);
        }
        if (commandType == CommandType.NEW_ORDER) {
            Side side = enumValue(Side.values(), mapped.getInt(base + SIDE_OFFSET));
            OrderType orderType = enumValue(OrderType.values(), mapped.getInt(base + ORDER_TYPE_OFFSET));
            long priceCents = mapped.getLong(base + PRICE_CENTS_OFFSET);
            Price price = priceCents == ABSENT_PRICE ? null : makePrice(priceCents);
            int quantity = mapped.getInt(base + QUANTITY_OFFSET);
            SelfTradePreventionMode stpMode = enumValue(SelfTradePreventionMode.values(),
                    mapped.getInt(base + STP_MODE_OFFSET));
            return new NewOrderCommand(sequenceNumber, inboundTimestamp, orderId, clientId, symbol, side, orderType,
                    price, quantity, stpMode);
        }
        return new JournalSnapshot(commandType, sequenceNumber, inboundTimestamp, orderId, clientId, symbol, 0);
    }

    private void writeFixedAscii(int lengthOffset, int valueOffset, int maxBytes, String value) {
        if (value == null) {
            mapped.putInt(lengthOffset, -1);
            clearBytes(valueOffset, maxBytes);
            return;
        }
        int length = value.length();
        if (length > maxBytes) {
            throw new IllegalArgumentException("Journal field exceeds fixed capacity of " + maxBytes + " bytes");
        }
        mapped.putInt(lengthOffset, length);
        int i = 0;
        for (; i < length; i++) {
            char c = value.charAt(i);
            mapped.put(valueOffset + i, c <= 0x7F ? (byte) c : (byte) '?');
        }
        for (; i < maxBytes; i++) {
            mapped.put(valueOffset + i, (byte) 0);
        }
    }

    private String readFixedAscii(int lengthOffset, int valueOffset, int maxBytes) {
        int length = mapped.getInt(lengthOffset);
        if (length < 0) {
            return null;
        }
        if (length > maxBytes) {
            throw new IllegalStateException("Mapped journal fixed string length exceeds capacity: " + length);
        }
        char[] chars = new char[length];
        for (int i = 0; i < length; i++) {
            chars[i] = (char) (mapped.get(valueOffset + i) & 0xFF);
        }
        return new String(chars);
    }

    private void clearBytes(int valueOffset, int maxBytes) {
        for (int i = 0; i < maxBytes; i++) {
            mapped.put(valueOffset + i, (byte) 0);
        }
    }

    private int checksum(int base) {
        int checksum = 0x811C9DC5;
        for (int i = RECORD_VERSION_OFFSET; i < CHECKSUM_OFFSET; i++) {
            checksum ^= mapped.get(base + i) & 0xFF;
            checksum *= 0x01000193;
        }
        return checksum;
    }

    private int recordOffset(long sequenceNumber) {
        return Math.toIntExact(HEADER_SIZE + ((sequenceNumber - 1L) * RECORD_SIZE));
    }

    private static void ensureFileSize(FileChannel channel, long fileSize) throws IOException {
        if (channel.size() >= fileSize) {
            return;
        }
        channel.position(fileSize - 1L);
        ByteBuffer oneByte = ByteBuffer.allocate(1);
        oneByte.put((byte) 0);
        oneByte.flip();
        while (oneByte.hasRemaining()) {
            channel.write(oneByte);
        }
        channel.force(true);
    }

    private static int ordinal(Enum<?> value) {
        return value == null ? ABSENT_ENUM : value.ordinal();
    }

    private static long priceCents(Price price) {
        return price == null ? ABSENT_PRICE : price.getCents();
    }

    private static Price makePrice(long cents) {
        try {
            return PriceFactory.makePrice(cents);
        } catch (Exception e) {
            throw new IllegalStateException("Mapped journal contains invalid price cents: " + cents, e);
        }
    }

    private static <T extends Enum<T>> T enumValue(T[] values, int ordinal) {
        if (ordinal == ABSENT_ENUM) {
            return null;
        }
        if (ordinal < 0 || ordinal >= values.length) {
            throw new IllegalStateException("Invalid enum ordinal in mapped journal: " + ordinal);
        }
        return values[ordinal];
    }

    private static int intSetting(String propertyName, String envName, int defaultValue) {
        String configured = System.getProperty(propertyName, System.getenv().getOrDefault(envName, ""));
        if (configured.isBlank()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(configured.trim());
        } catch (NumberFormatException e) {
            return defaultValue;
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
