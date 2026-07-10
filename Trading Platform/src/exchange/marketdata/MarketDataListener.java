package exchange.marketdata;

import java.util.List;

public interface MarketDataListener {
    default boolean hasSubscribers(String symbol) {
        return true;
    }

    void onL2Snapshot(L2Snapshot snapshot);

    void onTradeTape(String symbol, List<TradeRecord> trades);
}
