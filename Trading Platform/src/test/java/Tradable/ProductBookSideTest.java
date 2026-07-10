package Tradable;

import Exceptions.DataValidationException;
import Exceptions.InvalidPriceOperation;
import Exceptions.InvalidUserInput;
import Price.Price;
import Price.PriceFactory;
import User.UserManager;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static Tradable.BookSide.BUY;
import static Tradable.BookSide.SELL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class ProductBookSideTest {

    @BeforeAll
    static void setUpUsers() throws DataValidationException, InvalidUserInput {
        UserManager.getInstance().init(new String[] { "buyer_one", "buyer_two", "seller_one" });
    }

    @Test
    void buySideUsesHighestPriceAsTopOfBook() throws Exception {
        ProductBookSide side = new ProductBookSide(BUY);
        Price lowBid = PriceFactory.makePrice("$10.00");
        Price highBid = PriceFactory.makePrice("$10.25");

        side.add(new Order("buyer_one", "TST", lowBid, 40, BUY));
        side.add(new Order("buyer_two", "TST", highBid, 30, BUY));

        assertEquals(highBid, side.topOfBookPrice());
        assertEquals(30, side.topOfBookVolume());
    }

    @Test
    void sellSideUsesLowestPriceAsTopOfBook() throws Exception {
        ProductBookSide side = new ProductBookSide(SELL);
        Price lowAsk = PriceFactory.makePrice("$10.10");
        Price highAsk = PriceFactory.makePrice("$10.50");

        side.add(new Order("seller_one", "TST", highAsk, 25, SELL));
        side.add(new Order("buyer_one", "TST", lowAsk, 75, SELL));

        assertEquals(lowAsk, side.topOfBookPrice());
        assertEquals(75, side.topOfBookVolume());
    }

    @Test
    void tradeOutFillsOrdersAtSamePriceInFifoOrder()
            throws InvalidUserInput, InvalidPriceOperation, DataValidationException {
        ProductBookSide side = new ProductBookSide(BUY);
        Price price = PriceFactory.makePrice("$25.00");
        Order firstOrder = new Order("buyer_one", "TST", price, 50, BUY);
        Order secondOrder = new Order("buyer_two", "TST", price, 60, BUY);

        side.add(firstOrder);
        side.add(secondOrder);

        side.tradeOut(price, 75);

        assertEquals(50, firstOrder.getFilledVolume());
        assertEquals(0, firstOrder.getRemainingVolume());
        assertEquals(25, secondOrder.getFilledVolume());
        assertEquals(35, secondOrder.getRemainingVolume());
        assertEquals(price, side.topOfBookPrice());
        assertEquals(35, side.topOfBookVolume());
    }

    @Test
    void tradeOutRemovesEmptyPriceLevel() throws Exception {
        ProductBookSide side = new ProductBookSide(SELL);
        Price price = PriceFactory.makePrice("$30.00");

        side.add(new Order("seller_one", "TST", price, 10, SELL));
        side.tradeOut(price, 10);

        assertNull(side.topOfBookPrice());
        assertEquals(0, side.topOfBookVolume());
    }
}
