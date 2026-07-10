package exchange;

import Price.Price;
import Price.PriceFactory;
import Tradable.BookSide;
import Tradable.Order;
import User.ProductManager;
import User.UserManager;
import exchange.clearing.ClearingService;
import exchange.core.DeterministicMatchingEngine;
import exchange.core.ExchangeRuntime;
import exchange.core.Sequencer;
import exchange.dispatch.InMemoryEventDispatcher;
import exchange.gateway.GatewayValidator;
import exchange.gateway.OrderGateway;
import exchange.journal.InMemoryCommandJournal;
import exchange.model.ExchangeEvent;
import exchange.model.NewOrderCommand;
import exchange.model.OrderAccepted;
import exchange.model.OrderCancelled;
import exchange.model.OrderExecuted;
import exchange.model.OrderRejected;
import exchange.model.OrderType;
import exchange.model.RejectReason;
import exchange.model.SelfTradePreventionMode;
import exchange.model.Side;
import exchange.risk.ClientAccount;
import exchange.risk.InMemoryRiskEngine;
import exchange.risk.RiskProfile;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OrderGatewayPipelineTest {
    private final List<TestExchange> exchanges = new ArrayList<>();

    @AfterEach
    void tearDown() {
        for (TestExchange exchange : exchanges) {
            exchange.close();
        }
        exchanges.clear();
    }

    @Test
    void marketOrderSweepsThreeAskLevelsDeterministically() throws Exception {
        TestExchange exchange = newExchange();
        Price p1000 = PriceFactory.makePrice("$10.00");
        Price p1010 = PriceFactory.makePrice("$10.10");
        Price p1025 = PriceFactory.makePrice("$10.25");
        exchange.riskEngine.setPosition("SELLER1", "AAPL", 100);
        exchange.riskEngine.setPosition("SELLER2", "AAPL", 200);
        exchange.riskEngine.setPosition("SELLER3", "AAPL", 300);

        exchange.gateway.submit(limit("S1", "SELLER1", Side.SELL, p1000, 100));
        exchange.gateway.submit(limit("S2", "SELLER2", Side.SELL, p1010, 200));
        exchange.gateway.submit(limit("S3", "SELLER3", Side.SELL, p1025, 300));

        List<ExchangeEvent> events = exchange.gateway.submit(new NewOrderCommand(0, "MKT1", "BUYER1", "AAPL",
                Side.BUY, OrderType.MARKET, null, 450, SelfTradePreventionMode.CANCEL_NEWEST));

        List<OrderExecuted> buyerFills = events.stream()
                .filter(OrderExecuted.class::isInstance)
                .map(OrderExecuted.class::cast)
                .filter(event -> event.orderId().equals("MKT1"))
                .toList();

        assertEquals(3, buyerFills.size());
        assertEquals(100, buyerFills.get(0).fillQty());
        assertEquals(p1000, buyerFills.get(0).fillPrice());
        assertEquals(350, buyerFills.get(0).leavesQty());
        assertEquals(200, buyerFills.get(1).fillQty());
        assertEquals(p1010, buyerFills.get(1).fillPrice());
        assertEquals(150, buyerFills.get(1).leavesQty());
        assertEquals(150, buyerFills.get(2).fillQty());
        assertEquals(p1025, buyerFills.get(2).fillPrice());
        assertEquals(0, buyerFills.get(2).leavesQty());
        assertTrue(buyerFills.get(2).fullFill());

        assertEquals(4, exchange.journal.replay().size());
        assertEquals(1, exchange.journal.replay().get(0).sequenceNumber());
        assertEquals(4, exchange.journal.replay().get(3).sequenceNumber());
        assertEquals(10, exchange.dispatcher.events().size());
    }

    @Test
    void selfTradePreventionCancelsNewestAggressor() throws Exception {
        TestExchange exchange = newExchange();
        Price price = PriceFactory.makePrice("$20.00");
        exchange.riskEngine.setPosition("CLIENTA", "AAPL", 100);

        exchange.gateway.submit(limit("REST1", "CLIENTA", Side.SELL, price, 100));
        List<ExchangeEvent> events = exchange.gateway.submit(new NewOrderCommand(0, "BUY1", "CLIENTA", "AAPL",
                Side.BUY, OrderType.LIMIT, PriceFactory.makePrice("$21.00"), 50,
                SelfTradePreventionMode.CANCEL_NEWEST));

        assertFalse(events.stream().anyMatch(OrderExecuted.class::isInstance));
        OrderCancelled cancel = events.stream()
                .filter(OrderCancelled.class::isInstance)
                .map(OrderCancelled.class::cast)
                .findFirst()
                .orElseThrow();
        assertEquals("BUY1", cancel.orderId());
        assertEquals(50, cancel.cancelledQty());
        assertTrue(cancel.reason().contains("Self-trade prevention"));
    }

    @Test
    void killSwitchRejectsBeforeMatching() throws Exception {
        TestExchange exchange = newExchange();
        exchange.riskEngine.setGlobalKillSwitchEnabled(true);

        List<ExchangeEvent> events = exchange.gateway.submit(limit("B1", "BUYER1", Side.BUY,
                PriceFactory.makePrice("$10.00"), 10));

        OrderRejected rejected = assertInstanceOf(OrderRejected.class, events.get(0));
        assertEquals("B1", rejected.orderId());
        assertEquals(1, rejected.sequenceNumber());
        assertEquals(1, exchange.journal.replay().size());
    }

    @Test
    void rejectsBuyOrderWhenAvailableCashIsInsufficient() throws Exception {
        TestExchange exchange = newExchange();
        exchange.riskEngine.setAvailableCash("BUYER1", 9_999);

        List<ExchangeEvent> events = exchange.gateway.submit(limit("B1", "BUYER1", Side.BUY,
                PriceFactory.makePrice("$10.00"), 10));

        OrderRejected rejected = assertInstanceOf(OrderRejected.class, events.get(0));
        assertEquals(RejectReason.RISK_BUYING_POWER, rejected.reason());
        ClientAccount account = exchange.riskEngine.account("BUYER1");
        assertEquals(9_999, account.availableCashCents());
        assertEquals(0, account.reservedCashCents());
    }

    @Test
    void clearingUpdatesBuyerAndSellerBalancesAfterMatchedTrade() throws Exception {
        TestExchange exchange = newExchange();
        exchange.riskEngine.setAvailableCash("BUYER1", 200_000);
        exchange.riskEngine.setAvailableCash("SELLER1", 0);
        exchange.riskEngine.setPosition("SELLER1", "AAPL", 100);

        exchange.gateway.submit(limit("S1", "SELLER1", Side.SELL, PriceFactory.makePrice("$10.00"), 100));
        exchange.gateway.submit(limit("B1", "BUYER1", Side.BUY, PriceFactory.makePrice("$11.00"), 100));
        exchange.dispatcher.close();

        ClientAccount buyer = exchange.riskEngine.account("BUYER1");
        ClientAccount seller = exchange.riskEngine.account("SELLER1");
        assertEquals(100_000, buyer.availableCashCents());
        assertEquals(0, buyer.reservedCashCents());
        assertEquals(100, buyer.position("AAPL"));
        assertEquals(100_000, seller.availableCashCents());
        assertEquals(0, seller.position("AAPL"));
    }

    @Test
    void rejectsSellOrderWhenClientDoesNotHoldEnoughInventory() throws Exception {
        TestExchange exchange = newExchange();
        exchange.riskEngine.setPosition("WEB_TRADER", "AAPL", 10);

        List<ExchangeEvent> events = exchange.gateway.submit(limit("S1", "WEB_TRADER", Side.SELL,
                PriceFactory.makePrice("$10.00"), 11));

        OrderRejected rejected = assertInstanceOf(OrderRejected.class, events.get(0));
        assertEquals(RejectReason.RISK_POSITION_LIMIT, rejected.reason());
        assertEquals("Insufficient position to sell", rejected.message());
        assertEquals(10, exchange.riskEngine.account("WEB_TRADER").position("AAPL"));
        assertEquals(0, exchange.riskEngine.account("WEB_TRADER").reservedPosition("AAPL"));
    }

    @Test
    void gatewayAssignsPrimitiveSymbolIdBeforeJournaling() throws Exception {
        TestExchange exchange = newExchange();

        List<ExchangeEvent> events = exchange.gateway.submit(limit("B_SYMBOL_ID", "BUYER1", Side.BUY,
                PriceFactory.makePrice("$10.00"), 10));

        assertTrue(events.stream().anyMatch(OrderAccepted.class::isInstance));
        assertEquals(1, exchange.journal.replay().size());
        assertEquals(1, exchange.journal.replay().get(0).symbolId());
        assertEquals("AAPL", exchange.journal.replay().get(0).symbol());
    }

    @Test
    void dispatcherNotifiesListenersAsynchronously() throws Exception {
        TestExchange exchange = newExchange();
        CountDownLatch latch = new CountDownLatch(1);
        exchange.dispatcher.addListener(events -> {
            if (events.stream().anyMatch(OrderAccepted.class::isInstance)) {
                latch.countDown();
            }
        });

        exchange.gateway.submit(limit("ASYNC1", "BUYER1", Side.BUY, PriceFactory.makePrice("$10.00"), 10));

        assertTrue(latch.await(2, TimeUnit.SECONDS));
    }

    @Test
    void legacyProductManagerTrafficIsSequencedAndJournaled() throws Exception {
        UserManager.getInstance().init(new String[] { "LEGACY1" });
        ProductManager productManager = ProductManager.getInstance();
        try {
            productManager.addProduct("IBM");
        } catch (Exception ignored) {
        }

        int before = ExchangeRuntime.getInstance().journal().replay().size();
        ExchangeRuntime.getInstance().bootstrapFromPostgresIfAvailable();
        productManager.addTradable(new Order("LEGACY1", "IBM", PriceFactory.makePrice("$125.00"), 25, BookSide.BUY));
        int after = ExchangeRuntime.getInstance().journal().replay().size();

        assertEquals(before + 1, after);
        assertEquals("IBM", ExchangeRuntime.getInstance().journal().replay().get(after - 1).symbol());
    }

    private static NewOrderCommand limit(String orderId, String clientId, Side side, Price price, int quantity) {
        return new NewOrderCommand(0, orderId, clientId, "AAPL", side, OrderType.LIMIT, price, quantity,
                SelfTradePreventionMode.CANCEL_NEWEST);
    }

    private TestExchange newExchange() {
        InMemoryCommandJournal journal = new InMemoryCommandJournal(4_096);
        InMemoryEventDispatcher dispatcher = new InMemoryEventDispatcher();
        InMemoryRiskEngine riskEngine = new InMemoryRiskEngine(new RiskProfile(1_000_000_000L, 1_000_000,
                1_000_000_000L, false));
        dispatcher.addListener(new ClearingService(riskEngine));
        OrderGateway gateway = new OrderGateway(
                new GatewayValidator(Set.of("AAPL"), 1, 1, 1_000_000),
                riskEngine,
                new Sequencer(),
                journal,
                new DeterministicMatchingEngine(Set.of("AAPL")),
                dispatcher);
        TestExchange exchange = new TestExchange(gateway, journal, dispatcher, riskEngine);
        exchanges.add(exchange);
        return exchange;
    }

    private record TestExchange(
            OrderGateway gateway,
            InMemoryCommandJournal journal,
            InMemoryEventDispatcher dispatcher,
            InMemoryRiskEngine riskEngine) implements AutoCloseable {
        @Override
        public void close() {
            gateway.close();
            dispatcher.close();
            journal.close();
        }
    }
}
