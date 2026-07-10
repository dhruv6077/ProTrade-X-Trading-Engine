package exchange;

import Price.Price;
import Price.PriceFactory;
import exchange.model.ExchangeEvent;
import exchange.model.NewOrderCommand;
import exchange.model.OrderAccepted;
import exchange.model.OrderCancelled;
import exchange.model.OrderExecuted;
import exchange.model.OrderType;
import exchange.model.SelfTradePreventionMode;
import exchange.model.Side;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OrderBookCorrectnessTest {
    private ExchangeTestSupport.TestExchange exchange;

    @AfterEach
    void tearDown() {
        if (exchange != null) {
            exchange.close();
        }
    }

    @Test
    void fifoPriorityFillsSamePriceOrdersBySequence() throws Exception {
        exchange = ExchangeTestSupport.newExchange(Set.of("AAPL"));
        Price price = PriceFactory.makePrice("$100.00");

        exchange.gateway().submit(ExchangeTestSupport.limit("B1", "BUYER1", "AAPL", Side.BUY, price, 10));
        exchange.gateway().submit(ExchangeTestSupport.limit("B2", "BUYER2", "AAPL", Side.BUY, price, 10));
        exchange.riskEngine().setPosition("SELLER1", "AAPL", 20);

        List<ExchangeEvent> events = exchange.gateway().submit(ExchangeTestSupport.market("S1", "SELLER1",
                "AAPL", Side.SELL, 20));

        assertEquals(List.of("B1", "B2"), passiveFillOrder(events, Side.BUY));
    }

    @Test
    void partialFillConsumesMultipleRestingOrdersAndLeavesAggressorResting() throws Exception {
        exchange = ExchangeTestSupport.newExchange(Set.of("AAPL"));
        Price price = PriceFactory.makePrice("$100.00");
        exchange.riskEngine().setPosition("SELLER1", "AAPL", 10);
        exchange.riskEngine().setPosition("SELLER2", "AAPL", 15);

        exchange.gateway().submit(ExchangeTestSupport.limit("S1", "SELLER1", "AAPL", Side.SELL, price, 10));
        exchange.gateway().submit(ExchangeTestSupport.limit("S2", "SELLER2", "AAPL", Side.SELL, price, 15));

        List<ExchangeEvent> buyEvents = exchange.gateway().submit(ExchangeTestSupport.limit("B-LARGE", "BUYER1",
                "AAPL", Side.BUY, price, 40));

        List<OrderExecuted> buyerFills = executionsFor(buyEvents, "B-LARGE");
        assertEquals(List.of(10, 15), buyerFills.stream().map(OrderExecuted::fillQty).toList());
        assertEquals(15, buyerFills.get(1).leavesQty());
        assertTrue(buyEvents.stream()
                .filter(OrderAccepted.class::isInstance)
                .map(OrderAccepted.class::cast)
                .anyMatch(event -> event.order().leavesQty() == 40));

        exchange.riskEngine().setPosition("SELLER3", "AAPL", 15);
        List<ExchangeEvent> residualFill = exchange.gateway().submit(ExchangeTestSupport.market("S3", "SELLER3",
                "AAPL", Side.SELL, 15));

        OrderExecuted passiveResidual = executionsFor(residualFill, "B-LARGE").get(0);
        assertEquals(15, passiveResidual.fillQty());
        assertEquals(0, passiveResidual.leavesQty());
        assertTrue(passiveResidual.fullFill());
    }

    @Test
    void aggressiveLimitOrderSweepsOnlyPricesAtOrBetterThanLimitAndRestsResidual() throws Exception {
        exchange = ExchangeTestSupport.newExchange(Set.of("AAPL"));
        Price p10000 = PriceFactory.makePrice("$100.00");
        Price p10010 = PriceFactory.makePrice("$100.10");
        Price p10025 = PriceFactory.makePrice("$100.25");
        for (int level = 1; level <= 3; level++) {
            exchange.riskEngine().setPosition("SELLER" + level, "AAPL", 10);
        }
        exchange.gateway().submit(ExchangeTestSupport.limit("S1", "SELLER1", "AAPL", Side.SELL, p10000, 10));
        exchange.gateway().submit(ExchangeTestSupport.limit("S2", "SELLER2", "AAPL", Side.SELL, p10010, 10));
        exchange.gateway().submit(ExchangeTestSupport.limit("S3", "SELLER3", "AAPL", Side.SELL, p10025, 10));

        List<ExchangeEvent> events = exchange.gateway().submit(ExchangeTestSupport.limit("B-LIMIT", "BUYER1",
                "AAPL", Side.BUY, p10010, 35));

        List<OrderExecuted> buyerFills = executionsFor(events, "B-LIMIT");
        assertEquals(List.of(p10000, p10010), buyerFills.stream().map(OrderExecuted::fillPrice).toList());
        assertEquals(List.of(25, 15), buyerFills.stream().map(OrderExecuted::leavesQty).toList());
        assertFalse(buyerFills.stream().anyMatch(fill -> fill.fillPrice().equals(p10025)));

        exchange.riskEngine().setPosition("SELLER4", "AAPL", 15);
        List<ExchangeEvent> residualFill = exchange.gateway().submit(ExchangeTestSupport.market("S4", "SELLER4",
                "AAPL", Side.SELL, 15));
        assertEquals(15, executionsFor(residualFill, "B-LIMIT").get(0).fillQty());
    }

    @Test
    void iocFillsAvailableLiquidityAndCancelsResidualWithoutResting() throws Exception {
        exchange = ExchangeTestSupport.newExchange(Set.of("AAPL"));
        Price price = PriceFactory.makePrice("$100.00");
        exchange.riskEngine().setPosition("SELLER1", "AAPL", 10);
        exchange.gateway().submit(ExchangeTestSupport.limit("S1", "SELLER1", "AAPL", Side.SELL, price, 10));

        List<ExchangeEvent> events = exchange.gateway().submit(new NewOrderCommand(0, "B-IOC", "BUYER1", "AAPL",
                Side.BUY, OrderType.IOC, price, 25, SelfTradePreventionMode.CANCEL_NEWEST));

        assertEquals(10, executionsFor(events, "B-IOC").get(0).fillQty());
        OrderCancelled residual = events.stream()
                .filter(OrderCancelled.class::isInstance)
                .map(OrderCancelled.class::cast)
                .filter(event -> event.orderId().equals("B-IOC"))
                .findFirst()
                .orElseThrow();
        assertEquals(15, residual.cancelledQty());

        exchange.riskEngine().setPosition("SELLER2", "AAPL", 1);
        List<ExchangeEvent> probe = exchange.gateway().submit(ExchangeTestSupport.market("S-PROBE", "SELLER2",
                "AAPL", Side.SELL, 1));
        assertTrue(probe.stream().noneMatch(OrderExecuted.class::isInstance),
                "IOC residual must not rest on the book");
    }

    @Test
    void fokCancelsEntireOrderWhenFullQuantityIsUnavailable() throws Exception {
        exchange = ExchangeTestSupport.newExchange(Set.of("AAPL"));
        Price price = PriceFactory.makePrice("$100.00");
        exchange.riskEngine().setPosition("SELLER1", "AAPL", 10);
        exchange.gateway().submit(ExchangeTestSupport.limit("S1", "SELLER1", "AAPL", Side.SELL, price, 10));

        List<ExchangeEvent> events = exchange.gateway().submit(new NewOrderCommand(0, "B-FOK", "BUYER1", "AAPL",
                Side.BUY, OrderType.FOK, price, 25, SelfTradePreventionMode.CANCEL_NEWEST));

        assertTrue(events.stream().noneMatch(OrderExecuted.class::isInstance));
        OrderCancelled cancelled = assertInstanceOf(OrderCancelled.class, events.get(0));
        assertEquals("B-FOK", cancelled.orderId());
        assertEquals(25, cancelled.cancelledQty());
    }

    @Test
    void selfTradePreventionCancelOldestRemovesRestingOrderAndDoesNotExecuteWashTrade() throws Exception {
        exchange = ExchangeTestSupport.newExchange(Set.of("AAPL"));
        Price price = PriceFactory.makePrice("$100.00");
        exchange.riskEngine().setPosition("CLIENTA", "AAPL", 20);

        exchange.gateway().submit(ExchangeTestSupport.limit("S-REST", "CLIENTA", "AAPL", Side.SELL, price, 20));
        List<ExchangeEvent> events = exchange.gateway().submit(new NewOrderCommand(0, "B-STP", "CLIENTA", "AAPL",
                Side.BUY, OrderType.LIMIT, PriceFactory.makePrice("$101.00"), 20,
                SelfTradePreventionMode.CANCEL_OLDEST));

        assertTrue(events.stream().noneMatch(OrderExecuted.class::isInstance));
        OrderCancelled cancelledResting = events.stream()
                .filter(OrderCancelled.class::isInstance)
                .map(OrderCancelled.class::cast)
                .filter(event -> event.orderId().equals("S-REST"))
                .findFirst()
                .orElseThrow();
        assertEquals(20, cancelledResting.cancelledQty());
        assertTrue(cancelledResting.reason().contains("Self-trade prevention"));

        exchange.riskEngine().setPosition("SELLER1", "AAPL", 20);
        List<ExchangeEvent> fills = exchange.gateway().submit(ExchangeTestSupport.market("S1", "SELLER1",
                "AAPL", Side.SELL, 20));
        assertEquals(List.of("B-STP"), passiveFillOrder(fills, Side.BUY));
    }

    private static List<OrderExecuted> executionsFor(List<ExchangeEvent> events, String orderId) {
        return events.stream()
                .filter(OrderExecuted.class::isInstance)
                .map(OrderExecuted.class::cast)
                .filter(event -> event.orderId().equals(orderId))
                .toList();
    }

    private static List<String> passiveFillOrder(List<ExchangeEvent> events, Side passiveSide) {
        return events.stream()
                .filter(OrderExecuted.class::isInstance)
                .map(OrderExecuted.class::cast)
                .filter(event -> event.side() == passiveSide)
                .map(OrderExecuted::orderId)
                .toList();
    }
}
