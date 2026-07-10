package exchange.marketdata;

import java.time.Instant;
import java.util.List;

public record L2Snapshot(
        String symbol,
        List<L2Level> bids,
        List<L2Level> asks,
        Instant asOf) {

    public L2Snapshot {
        bids = List.copyOf(bids);
        asks = List.copyOf(asks);
    }

    public L2Snapshot top(int depth) {
        int bidDepth = Math.min(depth, bids.size());
        int askDepth = Math.min(depth, asks.size());
        return new L2Snapshot(symbol, bids.subList(0, bidDepth), asks.subList(0, askDepth), asOf);
    }

    public static L2Snapshot empty(String symbol) {
        return new L2Snapshot(symbol, List.of(), List.of(), Instant.EPOCH);
    }
}
