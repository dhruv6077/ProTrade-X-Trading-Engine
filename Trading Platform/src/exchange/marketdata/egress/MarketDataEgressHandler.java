package exchange.marketdata.egress;

import com.lmax.disruptor.EventHandler;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.util.ReferenceCountUtil;
import exchange.telemetry.LatencyTelemetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.LongFunction;

/**
 * Consumes primitive market-data deltas and bridges them to Netty.
 *
 * <p>Disruptor consumer thread: checks channel health and creates pooled Netty
 * buffers. Netty event-loop thread: performs the actual channel write/flush.
 * This handler never blocks on socket writability.</p>
 */
public final class MarketDataEgressHandler implements EventHandler<MarketDataDeltaEvent> {
    private static final Logger logger = LoggerFactory.getLogger(MarketDataEgressHandler.class);
    private static final long SLOW_CONSUMER_LOG_INTERVAL_NS = TimeUnit.SECONDS.toNanos(1);

    private final LongFunction<Channel[]> subscribersBySymbolId;
    private final Consumer<Channel> slowConsumerAction;
    private final SlowConsumerPolicy slowConsumerPolicy;
    private final AtomicLong droppedDeltas = new AtomicLong();
    private final AtomicLong closedChannels = new AtomicLong();
    private final AtomicLong lastSlowConsumerLogNs = new AtomicLong();

    public MarketDataEgressHandler(MarketDataChannelRegistry channelRegistry) {
        this(channelRegistry::channelsFor);
    }

    public MarketDataEgressHandler(LongFunction<Channel[]> subscribersBySymbolId) {
        this(subscribersBySymbolId, SlowConsumerPolicy.CLOSE, Channel::close);
    }

    public MarketDataEgressHandler(
            LongFunction<Channel[]> subscribersBySymbolId,
            SlowConsumerPolicy slowConsumerPolicy,
            Consumer<Channel> slowConsumerAction) {
        this.subscribersBySymbolId = Objects.requireNonNull(subscribersBySymbolId, "subscribersBySymbolId");
        this.slowConsumerPolicy = Objects.requireNonNull(slowConsumerPolicy, "slowConsumerPolicy");
        this.slowConsumerAction = Objects.requireNonNull(slowConsumerAction, "slowConsumerAction");
    }

    @Override
    public void onEvent(MarketDataDeltaEvent event, long sequence, boolean endOfBatch) {
        Channel[] subscribers = subscribersBySymbolId.apply(event.symbolId());
        if (subscribers == null || subscribers.length == 0) {
            return;
        }

        for (int i = 0; i < subscribers.length; i++) {
            Channel channel = subscribers[i];
            if (channel == null || !channel.isActive()) {
                continue;
            }
            if (!channel.isWritable()) {
                handleSlowConsumer(channel, event.symbolId(), event.sequenceId());
                continue;
            }
            writeDelta(channel, event, endOfBatch);
        }
    }

    public long droppedDeltas() {
        return droppedDeltas.get();
    }

    public long closedChannels() {
        return closedChannels.get();
    }

    private void writeDelta(Channel channel, MarketDataDeltaEvent event, boolean flush) {
        long ingressTimeNs = event.ingressTimeNs();
        long timestampNs = event.timestampNs();
        long symbolId = event.symbolId();
        long sequenceId = event.sequenceId();
        ByteBuf buffer = encode(channel, event);
        TextWebSocketFrame frame = new TextWebSocketFrame(buffer);
        channel.eventLoop().execute(() -> {
            if (!channel.isActive()) {
                ReferenceCountUtil.release(frame);
                return;
            }
            if (!channel.isWritable()) {
                ReferenceCountUtil.release(frame);
                handleSlowConsumer(channel, symbolId, sequenceId);
                return;
            }
            long writeTimeNs = System.nanoTime();
            LatencyTelemetry telemetry = LatencyTelemetry.getInstance();
            telemetry.recordTickToTrade(ingressTimeNs, writeTimeNs);
            telemetry.recordEgressWrite(timestampNs, writeTimeNs);
            channel.write(frame);
            if (flush) {
                channel.flush();
            }
        });
    }

    private void handleSlowConsumer(Channel channel, long symbolId, long sequenceId) {
        droppedDeltas.incrementAndGet();
        logSlowConsumer(symbolId, sequenceId);
        if (slowConsumerPolicy == SlowConsumerPolicy.CLOSE) {
            closedChannels.incrementAndGet();
            slowConsumerAction.accept(channel);
        }
    }

    private void logSlowConsumer(long symbolId, long sequenceId) {
        long now = System.nanoTime();
        long prior = lastSlowConsumerLogNs.get();
        if (now - prior < SLOW_CONSUMER_LOG_INTERVAL_NS) {
            return;
        }
        if (lastSlowConsumerLogNs.compareAndSet(prior, now)) {
            logger.warn(
                    "Market data egress slow consumer detected; policy={}, droppedDeltas={}, symbolId={}, sequenceId={}",
                    slowConsumerPolicy,
                    droppedDeltas.get(),
                    symbolId,
                    sequenceId);
        }
    }

    private static ByteBuf encode(Channel channel, MarketDataDeltaEvent event) {
        ByteBuf buffer = channel.alloc().buffer(192);
        writeAscii(buffer, "{\"type\":\"market-data-delta\",\"sequenceId\":");
        writeLong(buffer, event.sequenceId());
        writeAscii(buffer, ",\"symbolId\":");
        writeLong(buffer, event.symbolId());
        writeAscii(buffer, ",\"price\":");
        writeLong(buffer, event.price());
        writeAscii(buffer, ",\"size\":");
        writeLong(buffer, event.size());
        writeAscii(buffer, ",\"cumulativeSize\":");
        writeLong(buffer, event.cumulativeSize());
        writeAscii(buffer, ",\"side\":\"");
        writeAscii(buffer, sideName(event.side()));
        writeAscii(buffer, "\",\"updateType\":\"");
        writeAscii(buffer, updateTypeName(event.updateType()));
        writeAscii(buffer, "\",\"timestampNs\":");
        writeLong(buffer, event.timestampNs());
        writeByte(buffer, '}');
        return buffer;
    }

    private static String sideName(byte side) {
        return switch (side) {
            case MarketDataDeltaEvent.SIDE_BUY -> "bid";
            case MarketDataDeltaEvent.SIDE_SELL -> "ask";
            default -> "unknown";
        };
    }

    private static String updateTypeName(byte updateType) {
        return switch (updateType) {
            case MarketDataDeltaEvent.UPDATE_ADD -> "add";
            case MarketDataDeltaEvent.UPDATE_REPLACE -> "replace";
            case MarketDataDeltaEvent.UPDATE_DELETE -> "delete";
            case MarketDataDeltaEvent.UPDATE_TRADE -> "trade";
            default -> "unknown";
        };
    }

    private static void writeAscii(ByteBuf buffer, String value) {
        buffer.writeCharSequence(value, StandardCharsets.US_ASCII);
    }

    private static void writeLong(ByteBuf buffer, long value) {
        if (value == 0L) {
            writeByte(buffer, '0');
            return;
        }
        if (value == Long.MIN_VALUE) {
            writeAscii(buffer, "-9223372036854775808");
            return;
        }
        long n = value;
        if (n < 0) {
            writeByte(buffer, '-');
            n = -n;
        }

        long divisor = 1L;
        while (n / divisor >= 10L) {
            divisor *= 10L;
        }
        while (divisor > 0L) {
            writeByte(buffer, (int) ('0' + (n / divisor)));
            n %= divisor;
            divisor /= 10L;
        }
    }

    private static void writeByte(ByteBuf buffer, int value) {
        buffer.writeByte(value);
    }

    public enum SlowConsumerPolicy {
        DROP,
        CLOSE
    }
}
