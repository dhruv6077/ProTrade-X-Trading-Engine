package Tradable;

import Exceptions.InvalidPriceOperation;
import Exceptions.InvalidUserInput;
import Price.Price;

public class Quote {
    private final String user;
    private final String product;
    private final QuoteSide buySide;
    private final QuoteSide sellSide;

    public Quote(String symbol, Price buyPrice, int buyVolume, Price sellPrice, int sellVolume, String userName) throws InvalidUserInput, InvalidPriceOperation {


        this.buySide = new QuoteSide(userName, symbol, buyPrice, buyVolume, BookSide.BUY);
        this.sellSide = new QuoteSide(userName, symbol, sellPrice, sellVolume, BookSide.SELL);
        this.user = userName;
        this.product = symbol;

    }

    public QuoteSide getQuoteSide(BookSide sideIn) throws InvalidUserInput {

        if (sideIn == BookSide.BUY) {
            return this.buySide;
        } else if (sideIn == BookSide.SELL) {
            return this.sellSide;
        } else {
            throw new InvalidUserInput("Side cannot be null. Only BUY or SELL side allowed.");
        }
    }

    public String getSymbol() {
        return this.product;
    }

    public String getUser() {
        return this.user;
    }
}
