package Tradable;

import Exceptions.InvalidPriceOperation;
import Exceptions.InvalidUserInput;
import Price.Price;

public class QuoteSide implements Tradable {
    private final String user;
    private final String product;
    private final Price price;
    private final BookSide side;
    private final String id;
    private final int originalVolume;
    private int remainingVolume;
    private int cancelledVolume;
    private int filledVolume;
    private static final String userIdRegex = "[a-zA-Z]+";
    private static final String productIdRegex = "[a-zA-Z0-9.]*";

    public QuoteSide (String user, String product, Price price, int originalVolume, BookSide side) throws InvalidUserInput, InvalidPriceOperation {
        if (user == null || user.length() != 3 || !user.matches(userIdRegex)) {
            throw new InvalidUserInput("User.User Code is Invalid");
        }

        if (product == null || product.isEmpty() || product.length() > 5 || !product.matches(productIdRegex)) {
            throw new InvalidUserInput("Invalid product input.");
        }

        if (price == null) {
            throw new InvalidPriceOperation("Price cannot be null.");
        }

        if(side == null) {
            throw new InvalidUserInput("Bookside cannot be null.");
        }

        if(originalVolume <= 0 || originalVolume >= 10000) {
            throw new InvalidUserInput("The stock input is not in the specified range.");
        }





        this.user = user;
        this.product = product;
        this.price = price;
        this.side = side;
        this.id = user + product + price.toString() + System.nanoTime();;
        this.originalVolume = originalVolume;
        this.remainingVolume = originalVolume;
        this.cancelledVolume = 0;
        this.filledVolume = 0;

    }

    @Override
    public String getId() { return id; }

    @Override
    public int getCancelledVolume() {
        return cancelledVolume;
    }

    @Override
    public void setCancelledVolume(int newVol) {
        this.cancelledVolume = newVol;
    }

    @Override
    public int getRemainingVolume() {
        return remainingVolume;
    }

    @Override
    public void setRemainingVolume(int newVol) {
        this.remainingVolume = newVol;
    }

    @Override
    public TradableDTO makeTradableDTO() {
        return new TradableDTO(id, user, product, price, side, originalVolume, remainingVolume, cancelledVolume, filledVolume);
    }

    @Override
    public Price getPrice() {
        return price;
    }

    @Override
    public void setFilledVolume(int newVol) {
        this.filledVolume = newVol;
    }

    @Override
    public int getFilledVolume() {
        return filledVolume;
    }

    @Override
    public BookSide getSide() {
        return side;
    }

    @Override
    public String getUser() {
        return user;
    }

    @Override
    public String getProduct() {
        return product;
    }

    @Override
    public int getOriginalVolume() {
        return originalVolume;
    }

    @Override
    public String toString() {
        return "%s quote from %s: %s, Orig Vol: %d, Rem Vol: %d, Fill Vol: %d, CXL Vol: %d, ID: %s".formatted(product, user, price, originalVolume, remainingVolume, filledVolume, cancelledVolume, id);
    }
}
