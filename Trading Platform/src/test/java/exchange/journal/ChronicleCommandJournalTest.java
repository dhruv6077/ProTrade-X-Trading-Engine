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
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class ChronicleCommandJournalTest {
    @TempDir
    Path tempDir;

    @Test
    void persistsAndReplaysFullNewOrderCommands() throws Exception {
        assumeTrue(isChronicleRuntimeSupported(),
                "Chronicle Queue memory mapping is certified here on Java 21 LTS or with -DrunChronicleTests=true");
        Path journalPath = tempDir.resolve("commands");
        Instant firstTimestamp = Instant.parse("2026-06-29T12:00:00Z");
        Instant secondTimestamp = Instant.parse("2026-06-29T12:00:01Z");

        try (ChronicleCommandJournal journal = new ChronicleCommandJournal(journalPath, 1_024)) {
            journal.append(new NewOrderCommand(1, firstTimestamp, "B1", "BUYER1", "AAPL", Side.BUY,
                    OrderType.LIMIT, PriceFactory.makePrice("$101.25"), 25,
                    SelfTradePreventionMode.CANCEL_NEWEST));
            journal.append(new NewOrderCommand(2, secondTimestamp, "S1", "SELLER1", "AAPL", Side.SELL,
                    OrderType.MARKET, null, 10, SelfTradePreventionMode.DECREMENT_LARGER));
        }

        try (ChronicleCommandJournal journal = new ChronicleCommandJournal(journalPath, 1_024)) {
            List<OrderCommand> replayed = journal.replay();

            assertEquals(2, replayed.size());
            NewOrderCommand first = assertInstanceOf(NewOrderCommand.class, replayed.get(0));
            assertEquals(1L, first.sequenceNumber());
            assertEquals(firstTimestamp, first.inboundTimestamp());
            assertEquals("B1", first.orderId());
            assertEquals("BUYER1", first.clientId());
            assertEquals("AAPL", first.symbol());
            assertEquals(Side.BUY, first.side());
            assertEquals(OrderType.LIMIT, first.orderType());
            assertEquals(10_125L, first.price().getCents());
            assertEquals(25, first.quantity());
            assertEquals(SelfTradePreventionMode.CANCEL_NEWEST, first.stpMode());

            NewOrderCommand second = assertInstanceOf(NewOrderCommand.class, replayed.get(1));
            assertEquals(2L, second.sequenceNumber());
            assertEquals(secondTimestamp, second.inboundTimestamp());
            assertEquals("S1", second.orderId());
            assertEquals(Side.SELL, second.side());
            assertEquals(OrderType.MARKET, second.orderType());
            assertNull(second.price());
            assertEquals(10, second.quantity());
            assertEquals(SelfTradePreventionMode.DECREMENT_LARGER, second.stpMode());
        }
    }

    private static boolean isChronicleRuntimeSupported() {
        if (Boolean.getBoolean("runChronicleTests")) {
            return true;
        }
        int feature = Runtime.version().feature();
        return feature <= 21;
    }
}
