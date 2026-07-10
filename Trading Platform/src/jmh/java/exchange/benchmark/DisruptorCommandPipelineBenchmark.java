package exchange.benchmark;

import Price.Price;
import Price.PriceFactory;
import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import exchange.core.AffinityThreadFactory;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.time.Instant;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Measures the raw command handoff tier: producer thread -> LMAX Disruptor ring
 * slot -> single consumer. This deliberately excludes Netty, Javalin,
 * PostgreSQL, risk, and JSON so the result reflects the IPC boundary itself.
 */
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 5)
@Fork(1)
@State(Scope.Benchmark)
public class DisruptorCommandPipelineBenchmark {
    private static final int RING_BUFFER_SIZE = 65_536;
    private static final int ORDER_ID_MASK = RING_BUFFER_SIZE - 1;
    private static final String CLIENT_ID = "JMH_CLIENT";
    private static final String SYMBOL = "JMH";

    private final AtomicLong nextCommandSequence = new AtomicLong(1);
    private final AtomicLong consumedCommandSequence = new AtomicLong();
    private final AtomicLong consumedQuantity = new AtomicLong();

    private Disruptor<CommandSlot> disruptor;
    private RingBuffer<CommandSlot> ringBuffer;
    private String[] orderIds;
    private Price price;

    @Setup(Level.Trial)
    public void setUp() throws Exception {
        orderIds = new String[RING_BUFFER_SIZE];
        for (int i = 0; i < orderIds.length; i++) {
            orderIds[i] = "JMH-" + i;
        }
        price = PriceFactory.makePrice("100.00");

        disruptor = new Disruptor<>(
                CommandSlot.FACTORY,
                RING_BUFFER_SIZE,
                new AffinityThreadFactory("jmh-command-disruptor", true, false),
                ProducerType.MULTI,
                new BusySpinWaitStrategy());
        disruptor.handleEventsWith((slot, sequence, endOfBatch) -> {
            consumedQuantity.lazySet(slot.quantity);
            consumedCommandSequence.lazySet(slot.commandSequence);
        });
        ringBuffer = disruptor.start();
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        if (disruptor != null) {
            disruptor.shutdown();
        }
    }

    @Benchmark
    @Threads(1)
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public long singleProducerPublishOnly() {
        return publish();
    }

    @Benchmark
    @Threads(4)
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public long multiProducerPublishOnly() {
        return publish();
    }

    @Benchmark
    @Threads(1)
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public void publishToConsumerRoundTrip(Blackhole blackhole) {
        long commandSequence = publish();
        while (consumedCommandSequence.get() < commandSequence) {
            Thread.onSpinWait();
        }
        blackhole.consume(consumedQuantity.get());
    }

    private long publish() {
        long commandSequence = nextCommandSequence.getAndIncrement();
        long ringSequence = ringBuffer.next();
        try {
            CommandSlot slot = ringBuffer.get(ringSequence);
            slot.commandSequence = commandSequence;
            slot.inboundTimestamp = Instant.EPOCH;
            slot.orderId = orderIds[(int) commandSequence & ORDER_ID_MASK];
            slot.clientId = CLIENT_ID;
            slot.symbol = SYMBOL;
            slot.side = Side.BUY;
            slot.orderType = OrderType.LIMIT;
            slot.price = price;
            slot.quantity = 100;
            slot.stpMode = SelfTradePreventionMode.CANCEL_NEWEST;
        } finally {
            ringBuffer.publish(ringSequence);
        }
        return commandSequence;
    }

    public static final class CommandSlot {
        private static final EventFactory<CommandSlot> FACTORY = CommandSlot::new;

        private long commandSequence;
        private Instant inboundTimestamp;
        private String orderId;
        private String clientId;
        private String symbol;
        private Side side;
        private OrderType orderType;
        private Price price;
        private int quantity;
        private SelfTradePreventionMode stpMode;
    }
}
