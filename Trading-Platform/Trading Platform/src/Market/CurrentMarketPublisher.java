package Market;

import java.util.*;

import Exceptions.InvalidPriceOperation;

public class CurrentMarketPublisher {
    private static CurrentMarketPublisher instance;
    private final HashMap<String, ArrayList<CurrentMarketObserver>> filters = new HashMap<>();

    private CurrentMarketPublisher() {
    }

    public static CurrentMarketPublisher getInstance() {
        if (instance == null) {
            instance = new CurrentMarketPublisher();
        }
        return instance;
    }

    public void subscribeCurrentMarket(String symbol, CurrentMarketObserver cmo) {
        if (!filters.containsKey(symbol)) {
            filters.put(symbol, new ArrayList<>());
        }
        ArrayList<CurrentMarketObserver> getList = filters.get(symbol);
        getList.add(cmo);

    }

    public void unSubscribeCurrentMarket(String symbol, CurrentMarketObserver cmo) {
        if (!filters.containsKey(symbol)) {
            return;
        }
        ArrayList<CurrentMarketObserver> getList = filters.get(symbol);
        getList.remove(cmo);
    }

    void acceptCurrentMarket(String symbol, CurrentMarketSide buySide, CurrentMarketSide sellSide)
            throws InvalidPriceOperation {
        if (!filters.containsKey(symbol)) {
            return;
        }
        ArrayList<CurrentMarketObserver> getList = filters.get(symbol);

        for (CurrentMarketObserver eachCMOs : getList) {
            eachCMOs.updateCurrentMarket(symbol, buySide, sellSide);
        }
    }
}
