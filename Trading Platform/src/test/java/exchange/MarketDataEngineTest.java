package exchange;

import Price.PriceFactory;
import exchange.marketdata.CandleClosed;
import exchange.marketdata.L2Snapshot;
import exchange.marketdata.MarketDataEngine;
import exchange.marketdata.OhlcvCandle;
import exchange.model.OrderExecuted;
import exchange.model.Side;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

class MarketDataEngineTest {
    private ExchangeTestSupport.TestExchange exchange;
    private MarketDataEngine marketDataEngine;

    @AfterEach
    void tearDown() {
        if (exchange != null) {
            exchange.close();
        }
        if (marketDataEngine != null) {
            marketDataEngine.close();
        }
    }

    @Test
    void l2SnapshotAggregatesRemainingLeavesAfterPartialFills() throws Exception {
        exchange = ExchangeTestSupport.newExchange(Set.of("AAPL"));
        exchange.riskEngine().setPosition("SELLER1", "AAPL", 100);
        exchange.riskEngine().setPosition("SELLER2", "AAPL", 50);
        exchange.riskEngine().setPosition("SELLER3", "AAPL", 70);

        exchange.gateway().submit(ExchangeTestSupport.limit("S1", "SELLER1", "AAPL", Side.SELL,
                PriceFactory.makePrice("$10.00"), 100));
        exchange.gateway().submit(ExchangeTestSupport.limit("S2", "SELLER2", "AAPL", Side.SELL,
                PriceFactory.makePrice("$10.00"), 50));
        exchange.gateway().submit(ExchangeTestSupport.limit("S3", "SELLER3", "AAPL", Side.SELL,
                PriceFactory.makePrice("$11.00"), 70));
        exchange.gateway().submit(ExchangeTestSupport.limit("B1", "BUYER1", "AAPL", Side.BUY,
                PriceFactory.makePrice("$10.00"), 120));

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            L2Snapshot snapshot = exchange.marketDataEngine().l2Snapshot("AAPL", 5);
            assertEquals(2, snapshot.asks().size());
            assertEquals(PriceFactory.makePrice("$10.00"), snapshot.asks().get(0).price());
            assertEquals(30, snapshot.asks().get(0).quantity());
            assertEquals(PriceFactory.makePrice("$11.00"), snapshot.asks().get(1).price());
            assertEquals(70, snapshot.asks().get(1).quantity());
            assertEquals(0, snapshot.bids().size());
            assertEquals(2, exchange.marketDataEngine().tradeTape().size());
        });
    }

    @Test
    void ohlcvCandleClosesOnMinuteRolloverWithExactValues() throws Exception {
        marketDataEngine = new MarketDataEngine(Duration.ofMinutes(1));
        Instant minute0 = Instant.parse("2026-06-26T12:00:10Z");
        Instant minute1 = Instant.parse("2026-06-26T12:01:00Z");

        marketDataEngine.onEvents(List.of(
                execution(1, "T1", "T2", Side.BUY, "$10.00", 10, minute0),
                execution(2, "T3", "T4", Side.SELL, "$12.00", 20, minute0.plusSeconds(10)),
                execution(3, "T5", "T6", Side.BUY, "$9.00", 5, minute0.plusSeconds(20))));
        marketDataEngine.onEvents(List.of(execution(4, "T7", "T8", Side.BUY, "$11.00", 1, minute1)));

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            List<CandleClosed> closed = marketDataEngine.candleClosedEvents();
            assertEquals(1, closed.size());
            OhlcvCandle candle = closed.get(0).candle();
            assertEquals("AAPL", candle.symbol());
            assertEquals(Instant.parse("2026-06-26T12:00:00Z"), candle.windowStart());
            assertEquals(Instant.parse("2026-06-26T12:01:00Z"), candle.windowEnd());
            assertEquals(PriceFactory.makePrice("$10.00"), candle.open());
            assertEquals(PriceFactory.makePrice("$12.00"), candle.high());
            assertEquals(PriceFactory.makePrice("$9.00"), candle.low());
            assertEquals(PriceFactory.makePrice("$9.00"), candle.close());
            assertEquals(35, candle.volume());
        });
    }

    private static OrderExecuted execution(long sequenceNumber, String orderId, String contraOrderId, Side takerSide,
            String price, int quantity, Instant timestamp) throws Exception {
        return new OrderExecuted(sequenceNumber, orderId, "CLIENT1", "AAPL", contraOrderId, "CLIENT2", takerSide,
                PriceFactory.makePrice(price), quantity, 0, quantity, true, 0, 0, timestamp);
    }
}
