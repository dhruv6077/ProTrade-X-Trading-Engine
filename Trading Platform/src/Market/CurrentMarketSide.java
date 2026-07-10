package Market;

import Price.*;

public class CurrentMarketSide {
    private final int volume;
    private final Price price;

    public CurrentMarketSide(Price price, int volume) {
        this.price = price;
        this.volume = volume;
    }

    public Price getPrice() {
        return price;
    }

    public int getVolume() {
        return volume;
    }

    @Override
    public String toString() {
        return price + "x" + volume;
    }

}
