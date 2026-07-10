package exchange.marketdata.egress;

import com.lmax.disruptor.EventFactory;

/**
 * Pre-allocated market-data delta slot used by the downstream egress ring.
 *
 * <p>Publisher thread: the matching/MDE thread writes primitive fields in-place.
 * Consumer thread: Netty egress handlers read the same slot after publication.
 * Keep this schema primitive-only; no Strings, enums, records, or heap objects
 * belong on this event.</p>
 */
public final class MarketDataDeltaEvent {
    public static final byte SIDE_BUY = 1;
    public static final byte SIDE_SELL = 2;

    public static final byte UPDATE_ADD = 1;
    public static final byte UPDATE_REPLACE = 2;
    public static final byte UPDATE_DELETE = 3;
    public static final byte UPDATE_TRADE = 4;

    private long sequenceId;
    private long symbolId;
    private long price;
    private long size;
    private long cumulativeSize;
    private byte side;
    private byte updateType;
    private long timestampNs;
    private long ingressTimeNs;

    public void clear() {
        sequenceId = 0L;
        symbolId = 0L;
        price = 0L;
        size = 0L;
        cumulativeSize = 0L;
        side = 0;
        updateType = 0;
        timestampNs = 0L;
        ingressTimeNs = 0L;
    }

    void set(
            long sequenceId,
            long symbolId,
            long price,
            long size,
            long cumulativeSize,
            byte side,
            byte updateType,
            long timestampNs,
            long ingressTimeNs) {
        this.sequenceId = sequenceId;
        this.symbolId = symbolId;
        this.price = price;
        this.size = size;
        this.cumulativeSize = cumulativeSize;
        this.side = side;
        this.updateType = updateType;
        this.timestampNs = timestampNs;
        this.ingressTimeNs = ingressTimeNs;
    }

    public long sequenceId() {
        return sequenceId;
    }

    public long symbolId() {
        return symbolId;
    }

    public long price() {
        return price;
    }

    public long size() {
        return size;
    }

    public long cumulativeSize() {
        return cumulativeSize;
    }

    public byte side() {
        return side;
    }

    public byte updateType() {
        return updateType;
    }

    public long timestampNs() {
        return timestampNs;
    }

    public long ingressTimeNs() {
        return ingressTimeNs;
    }

    public static final class Factory implements EventFactory<MarketDataDeltaEvent> {
        public static final Factory INSTANCE = new Factory();

        private Factory() {
        }

        @Override
        public MarketDataDeltaEvent newInstance() {
            return new MarketDataDeltaEvent();
        }
    }
}
