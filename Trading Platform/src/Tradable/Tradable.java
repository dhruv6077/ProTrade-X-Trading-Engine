package Tradable;

import Price.Price;

public interface Tradable {
    String getId();
    int getCancelledVolume();
    void setCancelledVolume(int newVol);
    int getRemainingVolume();
    void setRemainingVolume(int newVol);
    TradableDTO makeTradableDTO();
    Price getPrice();
    void setFilledVolume(int newVol);
    int getFilledVolume();
    BookSide getSide();
    String getUser();
    String getProduct();
    int getOriginalVolume();

}
