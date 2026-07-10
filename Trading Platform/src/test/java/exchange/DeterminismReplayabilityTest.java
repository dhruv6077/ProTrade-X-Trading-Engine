package exchange;

import Price.Price;
import Price.PriceFactory;
import exchange.core.Sequencer;
import exchange.model.ExchangeEvent;
import exchange.model.OrderCommand;
import exchange.model.OrderExecuted;
import exchange.model.Side;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DeterminismReplayabilityTest {
    private ExchangeTestSupport.TestExchange exchange;

    @AfterEach
    void tearDown() {
        if (exchange != null) {
            exchange.close();
        }
    }

    @Test
    void priceTimePriorityFillsSamePriceOrdersInAssignedSequenceOrder() throws Exception {
        exchange = ExchangeTestSupport.newExchange(Set.of("AAPL"));
        Price price = PriceFactory.makePrice("$10.00");

        exchange.gateway().submit(ExchangeTestSupport.limit("B1", "BUYER1", "AAPL", Side.BUY, price, 10));
        exchange.gateway().submit(ExchangeTestSupport.limit("B2", "BUYER2", "AAPL", Side.BUY, price, 10));
        exchange.gateway().submit(ExchangeTestSupport.limit("B3", "BUYER3", "AAPL", Side.BUY, price, 10));
        exchange.riskEngine().setPosition("SELLER1", "AAPL", 30);

        List<ExchangeEvent> sweepEvents = exchange.gateway().submit(ExchangeTestSupport.market("S1", "SELLER1",
                "AAPL", Side.SELL, 30));

        List<String> passiveFillOrder = sweepEvents.stream()
                .filter(OrderExecuted.class::isInstance)
                .map(OrderExecuted.class::cast)
                .filter(event -> event.side() == Side.BUY)
                .map(OrderExecuted::orderId)
                .toList();
        assertEquals(List.of("B1", "B2", "B3"), passiveFillOrder);

        List<Long> passiveSequences = exchange.journal().replay().stream()
                .filter(command -> passiveFillOrder.contains(command.orderId()))
                .map(OrderCommand::sequenceNumber)
                .toList();
        assertEquals(List.of(1L, 2L, 3L), passiveSequences);
    }

    @Test
    void executionEventsUseSequencedInboundTimestampNotWallClockExecutionTime() throws Exception {
        Instant sequencedTimestamp = Instant.parse("2026-06-26T12:00:00Z");
        exchange = ExchangeTestSupport.newExchange(Set.of("AAPL"),
                new Sequencer(1, Clock.fixed(sequencedTimestamp, ZoneOffset.UTC)));
        exchange.riskEngine().setPosition("SELLER1", "AAPL", 10);

        exchange.gateway().submit(ExchangeTestSupport.limit("S1", "SELLER1", "AAPL", Side.SELL,
                PriceFactory.makePrice("$10.00"), 10));
        List<ExchangeEvent> events = exchange.gateway().submit(ExchangeTestSupport.limit("B1", "BUYER1", "AAPL",
                Side.BUY, PriceFactory.makePrice("$10.00"), 10));

        Instant commandTimestamp = exchange.journal().replay().stream()
                .filter(command -> command.orderId().equals("B1"))
                .findFirst()
                .orElseThrow()
                .inboundTimestamp();

        assertEquals(sequencedTimestamp, commandTimestamp);
        assertTrue(events.stream()
                .filter(OrderExecuted.class::isInstance)
                .map(OrderExecuted.class::cast)
                .allMatch(event -> event.eventTimestamp().equals(commandTimestamp)));
    }
}
