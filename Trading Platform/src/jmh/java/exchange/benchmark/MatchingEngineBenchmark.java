package exchange.benchmark;

import Price.Price;
import Price.PriceFactory;
import exchange.core.DeterministicMatchingEngine;
import exchange.model.ExchangeEvent;
import exchange.model.MutableOrderCommand;
import exchange.model.OrderType;
import exchange.model.SelfTradePreventionMode;
import exchange.model.Side;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * JMH harness for the {@link DeterministicMatchingEngine} hot path using a reused
 * {@link MutableOrderCommand} (zero-allocation order submission pattern).
 */
@Warmup(iterations = 3, time = 5)
@Measurement(iterations = 5, time = 10)
@Fork(1)
@State(Scope.Benchmark)
public class MatchingEngineBenchmark {
    private static final String SYMBOL = "BENCH";
    private static final String CLIENT = "BENCH_CLIENT";

    private DeterministicMatchingEngine engine;
    private MutableOrderCommand command;
    private Price bidPrice;
    private Price askPrice;
    private long sequence;

    @Setup(Level.Trial)
    public void setUpTrial() throws Exception {
        engine = new DeterministicMatchingEngine(Set.of(SYMBOL));
        command = new MutableOrderCommand();
        bidPrice = PriceFactory.makePrice("99.50");
        askPrice = PriceFactory.makePrice("100.50");

        seedLiquidity();
    }

    private void seedLiquidity() {
        for (int level = 0; level < 32; level++) {
            engine.process(newOrder("SEED-B-" + level, Side.BUY, bidPrice, 1_000));
            engine.process(newOrder("SEED-A-" + level, Side.SELL, askPrice, 1_000));
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public List<ExchangeEvent> throughputZeroAllocationNewOrder() {
        return processNextOrder();
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public List<ExchangeEvent> averageLatencyZeroAllocationNewOrder() {
        return processNextOrder();
    }

    private List<ExchangeEvent> processNextOrder() {
        long id = sequence++;
        Side side = (id & 1L) == 0L ? Side.BUY : Side.SELL;
        Price price = side == Side.BUY ? bidPrice : askPrice;
        command.populate("BM-" + id, CLIENT, SYMBOL, side, OrderType.LIMIT, price, 10,
                SelfTradePreventionMode.CANCEL_NEWEST);
        return engine.process(command);
    }

    private static exchange.model.NewOrderCommand newOrder(String orderId, Side side, Price price, int quantity) {
        return new exchange.model.NewOrderCommand(0, orderId, CLIENT, SYMBOL, side, OrderType.LIMIT, price, quantity,
                SelfTradePreventionMode.CANCEL_NEWEST);
    }
}
