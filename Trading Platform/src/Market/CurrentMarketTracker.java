package Market;

import Exceptions.InvalidPriceOperation;
import Price.*;

public class CurrentMarketTracker {
    private static CurrentMarketTracker instance;
    private CurrentMarketTracker() {

    }

    public static CurrentMarketTracker getInstance() {
        if(instance == null) {
            instance = new CurrentMarketTracker();
        }
        return instance;
    }

    public void updateMarket(String symbol, Price buyPrice, int buyVolume, Price sellPrice, int sellVolume) throws InvalidPriceOperation {
        Price marketWidth;
        if(buyPrice == null || sellPrice == null) {
            marketWidth = PriceFactory.makePrice(0);
            if(buyPrice == null) {
                buyPrice = PriceFactory.makePrice(0);
            }

            if(sellPrice == null) {
                sellPrice = PriceFactory.makePrice(0);
            }

        } else {

            marketWidth = sellPrice.subtract(buyPrice);
        }

        CurrentMarketSide buySide = new CurrentMarketSide(buyPrice, buyVolume);
        CurrentMarketSide sellSide = new CurrentMarketSide(sellPrice, sellVolume);

        System.out.println("******* Current Market *******");
        System.out.printf("* %s %sx%d - %sx%d [%s]%n",
                symbol, buyPrice.toString(), buyVolume, sellPrice.toString(), sellVolume, marketWidth);
        System.out.println("******************************");

        CurrentMarketPublisher.getInstance().acceptCurrentMarket(symbol, buySide, sellSide);
    }

}
