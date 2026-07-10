package exchange;

import Price.PriceFactory;
import exchange.core.MatchingEngine;
import exchange.core.Sequencer;
import exchange.dispatch.InMemoryEventDispatcher;
import exchange.gateway.GatewayValidator;
import exchange.gateway.OrderGateway;
import exchange.journal.InMemoryCommandJournal;
import exchange.model.ExchangeEvent;
import exchange.model.OrderRejected;
import exchange.model.RejectReason;
import exchange.model.Side;
import exchange.risk.ClientAccount;
import exchange.risk.InMemoryRiskEngine;
import exchange.risk.RiskProfile;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class ClearingSettlementTest {
    private ExchangeTestSupport.TestExchange exchange;
    private OrderGateway gateway;
    private InMemoryEventDispatcher dispatcher;

    @AfterEach
    void tearDown() {
        if (gateway != null) {
            gateway.close();
        }
        if (dispatcher != null) {
            dispatcher.close();
        }
        if (exchange != null) {
            exchange.close();
        }
    }

    @Test
    void buyOrderExceedingAvailableCashIsRejectedBeforeMatchingEngine() throws Exception {
        InMemoryCommandJournal journal = new InMemoryCommandJournal(4_096);
        dispatcher = new InMemoryEventDispatcher();
        InMemoryRiskEngine riskEngine = new InMemoryRiskEngine(new RiskProfile(1_000_000_000L, 1_000_000,
                1_000_000_000L, false));
        riskEngine.setAvailableCash("BUYER1", 99_999);
        MatchingEngine matchingEngine = mock(MatchingEngine.class);
        gateway = new OrderGateway(new GatewayValidator(Set.of("AAPL"), 1, 1, 1_000_000), riskEngine,
                new Sequencer(), journal, matchingEngine, dispatcher);

        List<ExchangeEvent> events = gateway.submit(ExchangeTestSupport.limit("B1", "BUYER1", "AAPL", Side.BUY,
                PriceFactory.makePrice("$100.00"), 10));

        OrderRejected rejected = assertInstanceOf(OrderRejected.class, events.get(0));
        assertEquals(RejectReason.RISK_BUYING_POWER, rejected.reason());
        verify(matchingEngine, never()).process(any());
        assertEquals(1, journal.replay().size());
    }

    @Test
    void matchedTradeSettlesCashPositionsAndUnusedLimitReserveExactly() throws Exception {
        exchange = ExchangeTestSupport.newExchange(Set.of("AAPL"));
        exchange.riskEngine().setAvailableCash("BUYER1", 100_000);
        exchange.riskEngine().setAvailableCash("SELLER1", 0);
        exchange.riskEngine().setPosition("SELLER1", "AAPL", 10);

        exchange.gateway().submit(ExchangeTestSupport.limit("S1", "SELLER1", "AAPL", Side.SELL,
                PriceFactory.makePrice("$100.00"), 5));
        exchange.gateway().submit(ExchangeTestSupport.limit("B1", "BUYER1", "AAPL", Side.BUY,
                PriceFactory.makePrice("$105.00"), 5));

        await().atMost(java.time.Duration.ofSeconds(5)).untilAsserted(() -> {
            ClientAccount buyer = exchange.riskEngine().account("BUYER1");
            ClientAccount seller = exchange.riskEngine().account("SELLER1");
            assertEquals(50_000, buyer.availableCashCents());
            assertEquals(0, buyer.reservedCashCents());
            assertEquals(5, buyer.position("AAPL"));
            assertEquals(50_000, seller.availableCashCents());
            assertEquals(5, seller.position("AAPL"));
        });
    }
}
