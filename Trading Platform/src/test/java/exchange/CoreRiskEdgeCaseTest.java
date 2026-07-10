package exchange;

import Price.Price;
import Price.PriceFactory;
import exchange.model.ExchangeEvent;
import exchange.model.ModifyOrderCommand;
import exchange.model.NewOrderCommand;
import exchange.model.OrderCancelled;
import exchange.model.OrderExecuted;
import exchange.model.OrderRejected;
import exchange.model.OrderRestated;
import exchange.model.OrderType;
import exchange.model.RejectReason;
import exchange.model.SelfTradePreventionMode;
import exchange.model.Side;
import exchange.risk.PreTradeRiskGuard;
import exchange.risk.RiskDecision;
import exchange.risk.RiskProfile;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CoreRiskEdgeCaseTest {
    private ExchangeTestSupport.TestExchange exchange;

    @AfterEach
    void tearDown() {
        if (exchange != null) {
            exchange.close();
        }
    }

    @Test
    void fatFingerOrderAboveMaxNotionalIsRejectedBeforeMatching() throws Exception {
        exchange = ExchangeTestSupport.newExchange(Set.of("AAPL"));

        List<ExchangeEvent> events = exchange.gateway().submit(ExchangeTestSupport.limit("B-FAT", "BUYER1", "AAPL",
                Side.BUY, PriceFactory.makePrice("$20000.00"), 1_000_000));

        OrderRejected rejected = assertInstanceOf(OrderRejected.class, events.get(0));
        assertEquals(RejectReason.RISK_NOTIONAL_LIMIT, rejected.reason());
        assertEquals(0, exchange.riskEngine().account("BUYER1").reservedCashCents());
    }

    @Test
    void preTradeRiskRejectsLimitOrdersOutsideConfiguredPriceBand() throws Exception {
        exchange = ExchangeTestSupport.newExchange(Set.of("AAPL"));
        exchange.riskEngine().setProfile("BUYER1", new RiskProfile(1_000_000_000L, 1_000_000,
                1_000_000_000L, false, 500));
        exchange.riskEngine().setReferencePrice("AAPL", Side.SELL, 10_000L);

        List<ExchangeEvent> events = exchange.gateway().submit(ExchangeTestSupport.limit("B-BAND", "BUYER1",
                "AAPL", Side.BUY, PriceFactory.makePrice("$106.00"), 1));

        OrderRejected rejected = assertInstanceOf(OrderRejected.class, events.get(0));
        assertEquals(RejectReason.RISK_PRICE_BAND, rejected.reason());
        assertEquals(0, exchange.riskEngine().account("BUYER1").reservedCashCents());
    }

    @Test
    void riskAcceptFastPathReusesSingletonDecision() throws Exception {
        PreTradeRiskGuard guard = new PreTradeRiskGuard();
        NewOrderCommand command = new NewOrderCommand(0, "B-OK", "BUYER1", "AAPL", Side.BUY,
                OrderType.LIMIT, PriceFactory.makePrice("$100.00"), 10,
                SelfTradePreventionMode.CANCEL_NEWEST);
        RiskProfile profile = new RiskProfile(1_000_000_000L, 1_000_000, 1_000_000_000L, false);

        RiskDecision first = guard.check(command, profile, 10_000L, false, true);
        RiskDecision second = guard.check(command, profile, 10_000L, false, true);

        assertTrue(first.accepted());
        assertTrue(first == second);
    }

    @Test
    void optionalPreTradeSelfTradeGuardRejectsSelfCrossBeforeMatching() throws Exception {
        exchange = ExchangeTestSupport.newExchange(Set.of("AAPL"));
        exchange.riskEngine().setPosition("CLIENTA", "AAPL", 25);
        exchange.riskEngine().setHardRejectSelfTrade(true);

        exchange.gateway().submit(ExchangeTestSupport.limit("S-REST", "CLIENTA", "AAPL", Side.SELL,
                PriceFactory.makePrice("$100.00"), 25));
        List<ExchangeEvent> events = exchange.gateway().submit(ExchangeTestSupport.limit("B-CROSS", "CLIENTA",
                "AAPL", Side.BUY, PriceFactory.makePrice("$101.00"), 25));

        OrderRejected rejected = assertInstanceOf(OrderRejected.class, events.get(0));
        assertEquals(RejectReason.WOULD_SELF_TRADE, rejected.reason());
        assertEquals(0, events.stream().filter(OrderExecuted.class::isInstance).count());
    }

    @Test
    void invalidZeroQuantityAndNegativePriceNeverReachTheBook() throws Exception {
        exchange = ExchangeTestSupport.newExchange(Set.of("AAPL"));

        List<ExchangeEvent> zeroQuantity = exchange.gateway().submit(ExchangeTestSupport.limit("B-ZERO", "BUYER1",
                "AAPL", Side.BUY, PriceFactory.makePrice("$10.00"), 0));
        List<ExchangeEvent> negativePrice = exchange.gateway().submit(ExchangeTestSupport.limit("B-NEG", "BUYER1",
                "AAPL", Side.BUY, PriceFactory.makePrice(-5_000), 10));

        assertEquals(RejectReason.INVALID_QUANTITY, assertInstanceOf(OrderRejected.class, zeroQuantity.get(0)).reason());
        assertEquals(RejectReason.INVALID_PRICE, assertInstanceOf(OrderRejected.class, negativePrice.get(0)).reason());
        assertEquals(0, exchange.riskEngine().account("BUYER1").reservedCashCents());
    }

    @Test
    void washTradePreventionCancelsAggressorWithoutExecution() throws Exception {
        exchange = ExchangeTestSupport.newExchange(Set.of("AAPL"));
        Price price = PriceFactory.makePrice("$100.00");
        exchange.riskEngine().setPosition("CLIENTA", "AAPL", 25);

        exchange.gateway().submit(ExchangeTestSupport.limit("S-REST", "CLIENTA", "AAPL", Side.SELL, price, 25));
        List<ExchangeEvent> events = exchange.gateway().submit(ExchangeTestSupport.limit("B-STP", "CLIENTA",
                "AAPL", Side.BUY, PriceFactory.makePrice("$101.00"), 25));

        assertFalse(events.stream().anyMatch(OrderExecuted.class::isInstance));
        OrderCancelled cancelled = events.stream()
                .filter(OrderCancelled.class::isInstance)
                .map(OrderCancelled.class::cast)
                .findFirst()
                .orElseThrow();
        assertEquals("B-STP", cancelled.orderId());
        assertTrue(cancelled.reason().contains("Self-trade prevention"));
    }

    @Test
    void marketOrderDeepSweepConsumesFiveAskLevelsInPriceOrder() throws Exception {
        exchange = ExchangeTestSupport.newExchange(Set.of("AAPL"));
        for (int level = 1; level <= 5; level++) {
            String seller = "SELLER" + level;
            exchange.riskEngine().setPosition(seller, "AAPL", 10);
            exchange.gateway().submit(ExchangeTestSupport.limit("S" + level, seller, "AAPL", Side.SELL,
                    PriceFactory.makePrice(10_00L + level), 10));
        }

        List<ExchangeEvent> events = exchange.gateway().submit(ExchangeTestSupport.market("B-SWEEP", "BUYER1",
                "AAPL", Side.BUY, 50));

        List<OrderExecuted> buyerFills = executionsFor(events, "B-SWEEP");
        assertEquals(5, buyerFills.size());
        for (int index = 0; index < buyerFills.size(); index++) {
            assertEquals(10, buyerFills.get(index).fillQty());
            assertEquals(PriceFactory.makePrice(10_01L + index), buyerFills.get(index).fillPrice());
        }
        assertTrue(buyerFills.get(4).fullFill());
    }

    @Test
    void iocPartiallyFillsAvailableQuantityAndCancelsResidual() throws Exception {
        exchange = ExchangeTestSupport.newExchange(Set.of("AAPL"));
        exchange.riskEngine().setPosition("SELLER1", "AAPL", 30);
        exchange.gateway().submit(ExchangeTestSupport.limit("S1", "SELLER1", "AAPL", Side.SELL,
                PriceFactory.makePrice("$10.00"), 30));

        List<ExchangeEvent> events = exchange.gateway().submit(new NewOrderCommand(0, "B-IOC", "BUYER1", "AAPL",
                Side.BUY, OrderType.IOC, PriceFactory.makePrice("$10.00"), 50,
                SelfTradePreventionMode.CANCEL_NEWEST));

        assertEquals(1, executionsFor(events, "B-IOC").size());
        assertEquals(30, executionsFor(events, "B-IOC").get(0).fillQty());
        OrderCancelled residual = events.stream()
                .filter(OrderCancelled.class::isInstance)
                .map(OrderCancelled.class::cast)
                .filter(event -> event.orderId().equals("B-IOC"))
                .findFirst()
                .orElseThrow();
        assertEquals(20, residual.cancelledQty());
        assertEquals("IOC residual cancelled", residual.reason());
    }

    @Test
    void fokWithOnlyPartialLiquidityCancelsWithoutAnyExecution() throws Exception {
        exchange = ExchangeTestSupport.newExchange(Set.of("AAPL"));
        exchange.riskEngine().setPosition("SELLER1", "AAPL", 30);
        exchange.gateway().submit(ExchangeTestSupport.limit("S1", "SELLER1", "AAPL", Side.SELL,
                PriceFactory.makePrice("$10.00"), 30));

        List<ExchangeEvent> events = exchange.gateway().submit(new NewOrderCommand(0, "B-FOK", "BUYER1", "AAPL",
                Side.BUY, OrderType.FOK, PriceFactory.makePrice("$10.00"), 50,
                SelfTradePreventionMode.CANCEL_NEWEST));

        assertFalse(events.stream().anyMatch(OrderExecuted.class::isInstance));
        OrderCancelled cancelled = assertInstanceOf(OrderCancelled.class, events.get(0));
        assertEquals("B-FOK", cancelled.orderId());
        assertEquals(50, cancelled.cancelledQty());
    }

    @Test
    void decreasingRestingOrderQuantityKeepsPriceTimePriority() throws Exception {
        exchange = ExchangeTestSupport.newExchange(Set.of("AAPL"));
        Price price = PriceFactory.makePrice("$10.00");

        exchange.gateway().submit(ExchangeTestSupport.limit("B1", "BUYER1", "AAPL", Side.BUY, price, 20));
        exchange.gateway().submit(ExchangeTestSupport.limit("B2", "BUYER2", "AAPL", Side.BUY, price, 10));
        List<ExchangeEvent> restate = exchange.gateway().process(new ModifyOrderCommand(0, "B1", "BUYER1",
                "AAPL", price, 10));
        exchange.riskEngine().setPosition("SELLER1", "AAPL", 20);

        assertInstanceOf(OrderRestated.class, restate.get(0));
        List<ExchangeEvent> sweep = exchange.gateway().submit(ExchangeTestSupport.market("S1", "SELLER1", "AAPL",
                Side.SELL, 20));

        assertEquals(List.of("B1", "B2"), passiveBuyFillOrder(sweep));
    }

    @Test
    void increasingRestingOrderQuantityMovesOrderToBackOfSamePriceQueue() throws Exception {
        exchange = ExchangeTestSupport.newExchange(Set.of("AAPL"));
        Price price = PriceFactory.makePrice("$10.00");

        exchange.gateway().submit(ExchangeTestSupport.limit("B1", "BUYER1", "AAPL", Side.BUY, price, 10));
        exchange.gateway().submit(ExchangeTestSupport.limit("B2", "BUYER2", "AAPL", Side.BUY, price, 10));
        List<ExchangeEvent> restate = exchange.gateway().process(new ModifyOrderCommand(0, "B1", "BUYER1",
                "AAPL", price, 15));
        exchange.riskEngine().setPosition("SELLER1", "AAPL", 20);

        assertInstanceOf(OrderRestated.class, restate.get(0));
        List<ExchangeEvent> sweep = exchange.gateway().submit(ExchangeTestSupport.market("S1", "SELLER1", "AAPL",
                Side.SELL, 20));

        assertEquals(List.of("B2", "B1"), passiveBuyFillOrder(sweep));
    }

    private static List<OrderExecuted> executionsFor(List<ExchangeEvent> events, String orderId) {
        return events.stream()
                .filter(OrderExecuted.class::isInstance)
                .map(OrderExecuted.class::cast)
                .filter(event -> event.orderId().equals(orderId))
                .toList();
    }

    private static List<String> passiveBuyFillOrder(List<ExchangeEvent> events) {
        return events.stream()
                .filter(OrderExecuted.class::isInstance)
                .map(OrderExecuted.class::cast)
                .filter(event -> event.side() == Side.BUY)
                .map(OrderExecuted::orderId)
                .toList();
    }
}
