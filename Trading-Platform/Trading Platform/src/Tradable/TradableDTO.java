package Tradable;

import Price.Price;

public class TradableDTO {
    public String user;
    public String product;
    public Price price;
    public BookSide side;
    public String id;
    public int originalVolume;
    public int remainingVolume;
    public int cancelledVolume;
    public int filledVolume;

    public TradableDTO(String id, String user, String product, Price price, BookSide side, int originalVolume, int remainingVolume, int cancelledVolume, int filledVolume) {
        this.user = user;
        this.product = product;
        this.price = price;
        this.side = side;
        this.id = id;
        this.originalVolume = originalVolume;
        this.remainingVolume = remainingVolume;
        this.cancelledVolume = cancelledVolume;
        this.filledVolume = filledVolume;
    }

    @Override
    public String toString() {
        return user + " order: " + side + " " + product + " at " + price + ", Orig Vol: " + originalVolume + ", Rem Vol: " + remainingVolume + ", Fill Vol: " + filledVolume + ", CXL Vol: " + cancelledVolume + ", ID: " + id;
    }
}
