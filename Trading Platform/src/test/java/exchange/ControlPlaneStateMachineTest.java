package exchange;

import Price.PriceFactory;
import exchange.core.DeterministicMatchingEngine;
import exchange.journal.MappedCommandJournal;
import exchange.model.AdminCommand;
import exchange.model.AdminEvent;
import exchange.model.AdminOperation;
import exchange.model.ExchangeEvent;
import exchange.model.NewOrderCommand;
import exchange.model.OrderCancelled;
import exchange.model.OrderRejected;
import exchange.model.OrderType;
import exchange.model.RejectReason;
import exchange.model.SelfTradePreventionMode;
import exchange.model.Side;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ControlPlaneStateMachineTest {
    @TempDir
    Path tempDir;

    private ExchangeTestSupport.TestExchange exchange;

    @AfterEach
    void tearDown() {
        if (exchange != null) {
            exchange.close();
        }
    }

    @Test
    void haltRejectsSubsequentOrdersUntilResumeInSequenceOrder() throws Exception {
        exchange = ExchangeTestSupport.newExchange(Set.of("AAPL"));

        List<ExchangeEvent> halt = exchange.gateway().submit(new AdminCommand(0, "HALT-1", "ADMIN1", "AAPL",
                AdminOperation.HALT_SYMBOL));
        List<ExchangeEvent> rejectedOrder = exchange.gateway().submit(ExchangeTestSupport.limit("B-HALTED",
                "BUYER1", "AAPL", Side.BUY, PriceFactory.makePrice("$100.00"), 10));
        List<ExchangeEvent> resume = exchange.gateway().submit(new AdminCommand(0, "RESUME-1", "ADMIN1", "AAPL",
                AdminOperation.RESUME_SYMBOL));
        List<ExchangeEvent> acceptedOrder = exchange.gateway().submit(ExchangeTestSupport.limit("B-ACTIVE",
                "BUYER1", "AAPL", Side.BUY, PriceFactory.makePrice("$100.00"), 10));

        AdminEvent haltEvent = assertInstanceOf(AdminEvent.class, halt.get(0));
        assertEquals(1L, haltEvent.sequenceNumber());
        assertEquals(AdminOperation.HALT_SYMBOL, haltEvent.operation());

        OrderRejected rejected = assertInstanceOf(OrderRejected.class, rejectedOrder.get(0));
        assertEquals(2L, rejected.sequenceNumber());
        assertEquals(RejectReason.MARKET_HALTED, rejected.reason());

        AdminEvent resumeEvent = assertInstanceOf(AdminEvent.class, resume.get(0));
        assertEquals(3L, resumeEvent.sequenceNumber());
        assertEquals(AdminOperation.RESUME_SYMBOL, resumeEvent.operation());

        assertEquals(4L, acceptedOrder.get(0).sequenceNumber());
        assertEquals(List.of(1L, 2L, 3L, 4L), exchange.journal().replay().stream()
                .map(command -> command.sequenceNumber())
                .toList());
    }

    @Test
    void massCancelForClientRunsOnCoreThreadAndRemovesRestingOrders() throws Exception {
        exchange = ExchangeTestSupport.newExchange(Set.of("AAPL"));

        exchange.gateway().submit(ExchangeTestSupport.limit("B1", "BUYER1", "AAPL", Side.BUY,
                PriceFactory.makePrice("$10.00"), 10));
        exchange.gateway().submit(ExchangeTestSupport.limit("B2", "BUYER1", "AAPL", Side.BUY,
                PriceFactory.makePrice("$9.90"), 20));

        List<ExchangeEvent> massCancel = exchange.gateway().submit(new AdminCommand(0, "MC-1", "BUYER1", "AAPL",
                AdminOperation.CANCEL_ALL_FOR_CLIENT));

        assertInstanceOf(AdminEvent.class, massCancel.get(0));
        List<OrderCancelled> cancels = massCancel.stream()
                .filter(OrderCancelled.class::isInstance)
                .map(OrderCancelled.class::cast)
                .toList();
        assertEquals(2, cancels.size());
        assertTrue(cancels.stream().allMatch(cancel -> cancel.reason().equals("Admin mass cancel")));

        exchange.riskEngine().setPosition("SELLER1", "AAPL", 50);
        List<ExchangeEvent> sell = exchange.gateway().submit(ExchangeTestSupport.market("S1", "SELLER1", "AAPL",
                Side.SELL, 50));
        assertTrue(sell.stream().noneMatch(event -> event instanceof exchange.model.OrderExecuted));
    }

    @Test
    void mappedJournalReplaysAdminCommandsIntoDeterministicStateMachine() throws Exception {
        Path journalPath = tempDir.resolve("control-plane.mmap");
        try (MappedCommandJournal journal = new MappedCommandJournal(journalPath, 128)) {
            journal.append(new AdminCommand(1, java.time.Instant.EPOCH, "HALT-1", "ADMIN1", "AAPL",
                    AdminOperation.HALT_SYMBOL));
            journal.append(new NewOrderCommand(2, java.time.Instant.EPOCH, "B-HALTED", "BUYER1", "AAPL",
                    Side.BUY, OrderType.LIMIT, PriceFactory.makePrice("$100.00"), 10,
                    SelfTradePreventionMode.CANCEL_NEWEST));
        }

        List<ExchangeEvent> replayedEvents = new ArrayList<>();
        try (MappedCommandJournal journal = new MappedCommandJournal(journalPath, 128)) {
            DeterministicMatchingEngine engine = new DeterministicMatchingEngine(Set.of("AAPL"));
            for (exchange.model.OrderCommand command : journal.replay()) {
                replayedEvents.addAll(engine.process(command));
            }
        }

        assertInstanceOf(AdminEvent.class, replayedEvents.get(0));
        OrderRejected rejected = assertInstanceOf(OrderRejected.class, replayedEvents.get(1));
        assertEquals(2L, rejected.sequenceNumber());
        assertEquals(RejectReason.MARKET_HALTED, rejected.reason());
    }
}
