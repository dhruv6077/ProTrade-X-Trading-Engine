package User;

import Exceptions.InvalidUserInput;
import Market.CurrentMarketObserver;
import Market.CurrentMarketSide;

import Tradable.TradableDTO;

import java.util.HashMap;

public class User implements CurrentMarketObserver {
    private String userId;
    private final HashMap<String, TradableDTO> tradables = new HashMap<>();
    private final HashMap<String, CurrentMarketSide[]> currentMarkets = new HashMap<>();
    private static final String userIdRegex = "[a-zA-Z0-9_]+";

    public User(String userId) throws InvalidUserInput {
        setUserId(userId);
    }

    public String getUserId() {
        return userId;
    }

    private void setUserId(String userId) throws InvalidUserInput {
        if (userId == null || userId.length() < 3 || userId.length() > 20 || !userId.matches(userIdRegex)) {
            throw new InvalidUserInput("userId entered is not valid (must be 3-20 alphanumeric chars): " + userId);
        }

        this.userId = userId;
    }

    public void addTradable(TradableDTO o) {
        if (o != null) {
            tradables.put(o.id, o);
        }
    }

    public boolean hasTradableWithRemainingQty() {

        for (TradableDTO eachTradable : tradables.values()) {
            if (eachTradable.remainingVolume > 0) {
                return true;
            }
        }
        return false;
    }

    public TradableDTO getTradableWithRemainingQty() {
        for (TradableDTO eachTradable : tradables.values()) {
            if (eachTradable.remainingVolume > 0) {
                return eachTradable;
            }
        }
        return null;
    }

    @Override
    public String toString() {
        System.out.println("User Id: " + userId);
        for (TradableDTO allTradables : tradables.values()) {
            System.out.println("Product: " + allTradables.product + " Price: " + allTradables.price
                    + " OriginalVolume: " + allTradables.originalVolume + " RemainingVolume: "
                    + allTradables.remainingVolume + " CancelledVolume: " + allTradables.cancelledVolume
                    + " FilledVolume: " + allTradables.filledVolume + " User: " + allTradables.user
                    + " Side: " + allTradables.side + " Id: " + allTradables.id);
        }
        return "";
    }

    @Override
    public void updateCurrentMarket(String symbol, CurrentMarketSide buySide, CurrentMarketSide sellSide) {
        CurrentMarketSide[] currentMarketSides = { buySide, sellSide };
        currentMarkets.put(symbol, currentMarketSides);
    }

    public String getCurrentMarkets() {
        StringBuilder sb = new StringBuilder();
        for (String symbol : currentMarkets.keySet()) {
            CurrentMarketSide[] marketData = currentMarkets.get(symbol);
            sb.append(symbol).append(" ").append(marketData[0]).append(" - ").append(marketData[1]).append("\n");
        }
        return sb.toString();
    }
}
