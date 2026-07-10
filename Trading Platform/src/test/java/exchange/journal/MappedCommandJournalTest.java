package exchange.journal;

import Price.PriceFactory;
import exchange.model.NewOrderCommand;
import exchange.model.OrderCommand;
import exchange.model.OrderType;
import exchange.model.SelfTradePreventionMode;
import exchange.model.Side;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class MappedCommandJournalTest {
    @TempDir
    Path tempDir;

    @Test
    void forcesAndReplaysSequencedNewOrderCommands() throws Exception {
        Path journalPath = tempDir.resolve("commands.mmap");
        Instant firstTimestamp = Instant.parse("2026-06-30T10:15:30.123456789Z");
        Instant secondTimestamp = Instant.parse("2026-06-30T10:15:31Z");

        try (MappedCommandJournal journal = new MappedCommandJournal(journalPath, 128)) {
            journal.append(new NewOrderCommand(1, firstTimestamp, "BUY-1", "CLIENT-A", "AAPL", Side.BUY,
                    OrderType.LIMIT, PriceFactory.makePrice("$187.42"), 40,
                    SelfTradePreventionMode.CANCEL_NEWEST));
            journal.append(new NewOrderCommand(2, secondTimestamp, "SELL-1", "CLIENT-B", "AAPL", Side.SELL,
                    OrderType.MARKET, null, 15, SelfTradePreventionMode.DECREMENT_LARGER));
            assertEquals(2, journal.totalAppended());
        }

        try (MappedCommandJournal journal = new MappedCommandJournal(journalPath, 128)) {
            List<OrderCommand> replayed = journal.replay();

            assertEquals(2, replayed.size());
            NewOrderCommand first = assertInstanceOf(NewOrderCommand.class, replayed.get(0));
            assertEquals(1L, first.sequenceNumber());
            assertEquals(firstTimestamp, first.inboundTimestamp());
            assertEquals("BUY-1", first.orderId());
            assertEquals("CLIENT-A", first.clientId());
            assertEquals("AAPL", first.symbol());
            assertEquals(Side.BUY, first.side());
            assertEquals(OrderType.LIMIT, first.orderType());
            assertEquals(18_742L, first.price().getCents());
            assertEquals(40, first.quantity());
            assertEquals(SelfTradePreventionMode.CANCEL_NEWEST, first.stpMode());

            NewOrderCommand second = assertInstanceOf(NewOrderCommand.class, replayed.get(1));
            assertEquals(2L, second.sequenceNumber());
            assertEquals(secondTimestamp, second.inboundTimestamp());
            assertEquals("SELL-1", second.orderId());
            assertEquals("CLIENT-B", second.clientId());
            assertEquals(Side.SELL, second.side());
            assertEquals(OrderType.MARKET, second.orderType());
            assertNull(second.price());
            assertEquals(15, second.quantity());
            assertEquals(SelfTradePreventionMode.DECREMENT_LARGER, second.stpMode());
        }
    }

    @Test
    void rejectsCommandsWithoutSequencingAndCapacityOverflow() throws Exception {
        Path journalPath = tempDir.resolve("capacity.mmap");

        try (MappedCommandJournal journal = new MappedCommandJournal(journalPath, 1)) {
            assertThrows(IllegalArgumentException.class, () -> journal.append(new NewOrderCommand(0,
                    Instant.EPOCH, "BAD", "CLIENT", "AAPL", Side.BUY, OrderType.LIMIT,
                    PriceFactory.makePrice("$1.00"), 1, SelfTradePreventionMode.CANCEL_NEWEST)));

            journal.append(new NewOrderCommand(1, Instant.EPOCH, "OK", "CLIENT", "AAPL", Side.BUY,
                    OrderType.LIMIT, PriceFactory.makePrice("$1.00"), 1,
                    SelfTradePreventionMode.CANCEL_NEWEST));

            assertThrows(IllegalStateException.class, () -> journal.append(new NewOrderCommand(2,
                    Instant.EPOCH, "OVERFLOW", "CLIENT", "AAPL", Side.BUY, OrderType.LIMIT,
                    PriceFactory.makePrice("$1.00"), 1, SelfTradePreventionMode.CANCEL_NEWEST)));
        }
    }
}
