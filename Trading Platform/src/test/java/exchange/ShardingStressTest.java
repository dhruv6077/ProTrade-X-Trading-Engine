package exchange;

import Price.PriceFactory;
import exchange.model.ExchangeEvent;
import exchange.model.OrderAccepted;
import exchange.model.OrderCommand;
import exchange.model.OrderRejected;
import exchange.model.RejectReason;
import exchange.model.Side;
import exchange.risk.ClientAccount;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ShardingStressTest {
    private ExchangeTestSupport.TestExchange exchange;
    private ExecutorService executor;

    @AfterEach
    void tearDown() {
        if (executor != null) {
            executor.shutdownNow();
        }
        if (exchange != null) {
            exchange.close();
        }
    }

    @Test
    void concurrentOrdersAcrossSymbolShardsAreSequencedAndDispatchedWithoutDrops() throws Exception {
        Set<String> symbols = Set.of("BTC", "ETH", "SOL");
        exchange = ExchangeTestSupport.newExchange(symbols);
        for (int clientIndex = 0; clientIndex < 10; clientIndex++) {
            exchange.riskEngine().setShortSellingEnabled("CLIENT" + clientIndex, true);
        }

        int ordersPerSymbol = 200;
        int expectedOrders = symbols.size() * ordersPerSymbol;
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(expectedOrders);
        CountDownLatch acceptedEvents = new CountDownLatch(expectedOrders);
        ConcurrentLinkedQueue<Throwable> failures = new ConcurrentLinkedQueue<>();
        AtomicInteger acceptedCount = new AtomicInteger();

        exchange.dispatcher().addListener(events -> {
            for (ExchangeEvent event : events) {
                if (event instanceof OrderAccepted) {
                    acceptedCount.incrementAndGet();
                    acceptedEvents.countDown();
                }
            }
        });

        executor = Executors.newFixedThreadPool(24);
        int orderIndex = 0;
        for (String symbol : symbols) {
            for (int i = 0; i < ordersPerSymbol; i++) {
                int id = ++orderIndex;
                executor.submit(() -> {
                    try {
                        assertTrue(start.await(5, TimeUnit.SECONDS));
                        exchange.gateway().submit(ExchangeTestSupport.limit("ORD" + id, "CLIENT" + (id % 10),
                                symbol, Side.SELL, PriceFactory.makePrice("$10.00"), 1));
                    } catch (Throwable t) {
                        failures.add(t);
                    } finally {
                        done.countDown();
                    }
                });
            }
        }

        start.countDown();
        assertTrue(done.await(10, TimeUnit.SECONDS));
        assertTrue(failures.isEmpty(), () -> failures.peek().toString());
        assertTrue(acceptedEvents.await(10, TimeUnit.SECONDS));

        await().atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() -> assertEquals(expectedOrders, exchange.dispatcher().events().size()));

        List<OrderCommand> journal = exchange.journal().replay();
        assertEquals(expectedOrders, journal.size());
        Set<Long> sequences = new HashSet<>();
        for (OrderCommand command : journal) {
            sequences.add(command.sequenceNumber());
        }
        assertEquals(expectedOrders, sequences.size());
        for (long sequence = 1; sequence <= expectedOrders; sequence++) {
            assertTrue(sequences.contains(sequence), "missing sequence " + sequence);
        }
        assertEquals(expectedOrders, acceptedCount.get());
    }

    @Test
    void stageTwoShardRoutingUsesPrimitiveSymbolIds() throws Exception {
        Set<String> symbols = Set.of("BTC", "ETH", "SOL");
        exchange = ExchangeTestSupport.newExchange(symbols);

        exchange.gateway().submit(ExchangeTestSupport.limit("BTC1", "CLIENT1", "BTC", Side.BUY,
                PriceFactory.makePrice("$10.00"), 1));
        exchange.gateway().submit(ExchangeTestSupport.limit("ETH1", "CLIENT1", "ETH", Side.BUY,
                PriceFactory.makePrice("$10.00"), 1));
        exchange.gateway().submit(ExchangeTestSupport.limit("SOL1", "CLIENT1", "SOL", Side.BUY,
                PriceFactory.makePrice("$10.00"), 1));

        List<OrderCommand> journal = exchange.journal().replay();
        assertEquals(3, journal.size());
        Set<Integer> symbolIds = new HashSet<>();
        for (OrderCommand command : journal) {
            assertTrue(command.symbolId() > 0, "missing symbolId for " + command.symbol());
            symbolIds.add(command.symbolId());
        }
        assertEquals(3, symbolIds.size());
        assertEquals(3, exchange.gateway().shardStatuses().size());
    }

    @Test
    void concurrentCrossSymbolOrdersForSameClientCannotOverReserveCash() throws Exception {
        Set<String> symbols = Set.of("BTC", "ETH", "SOL");
        exchange = ExchangeTestSupport.newExchange(symbols);
        exchange.riskEngine().setAvailableCash("CLIENT_RISK", 10_000L);

        int orderCount = 90;
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(orderCount);
        ConcurrentLinkedQueue<Throwable> failures = new ConcurrentLinkedQueue<>();
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger rejectedForCash = new AtomicInteger();

        executor = Executors.newFixedThreadPool(24);
        String[] symbolArray = symbols.toArray(String[]::new);
        for (int i = 0; i < orderCount; i++) {
            int id = i + 1;
            String symbol = symbolArray[i % symbolArray.length];
            executor.submit(() -> {
                try {
                    assertTrue(start.await(5, TimeUnit.SECONDS));
                    List<ExchangeEvent> events = exchange.gateway().submit(ExchangeTestSupport.limit(
                            "RISK" + id, "CLIENT_RISK", symbol, Side.BUY,
                            PriceFactory.makePrice("$10.00"), 1));
                    if (events.stream().anyMatch(OrderAccepted.class::isInstance)) {
                        accepted.incrementAndGet();
                    }
                    events.stream()
                            .filter(OrderRejected.class::isInstance)
                            .map(OrderRejected.class::cast)
                            .filter(event -> event.reason() == RejectReason.RISK_BUYING_POWER)
                            .findAny()
                            .ifPresent(ignored -> rejectedForCash.incrementAndGet());
                } catch (Throwable t) {
                    failures.add(t);
                } finally {
                    done.countDown();
                }
            });
        }

        start.countDown();
        assertTrue(done.await(10, TimeUnit.SECONDS));
        assertTrue(failures.isEmpty(), () -> failures.peek().toString());

        ClientAccount account = exchange.riskEngine().account("CLIENT_RISK");
        assertEquals(10, accepted.get());
        assertEquals(orderCount - 10, rejectedForCash.get());
        assertEquals(0, account.availableCashCents());
        assertEquals(10_000L, account.reservedCashCents());
        assertEquals(orderCount, exchange.journal().replay().size());
    }
}
