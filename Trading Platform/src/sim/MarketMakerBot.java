package sim;

import Tradable.BookSide;

import java.util.List;

/**
 * Market Maker Bot providing liquidity to the market.
 * Places both BUY and SELL orders around a base price.
 */
public class MarketMakerBot extends BaseBot {
    private final List<String> products;
    private final double spread;

    public MarketMakerBot(String botId, List<String> products, long sleepInterval) {
        super(botId, sleepInterval);
        this.products = products;
        this.spread = 0.05; // 5% spread
    }

    @Override
    protected void executeStrategy() throws Exception {
        String product = products.get(random.nextInt(products.size()));

        // In a real system, we'd get the last trade price or mid-market price.
        // For simulation, we'll use a random walk from a base price or just random.
        // Let's simulate a price around $100-$200 for simplicity if no market data.

        double basePrice = 100.0 + random.nextDouble() * 100.0;

        // Place Buy Order
        double buyPrice = basePrice * (1 - (spread / 2));
        int buyVol = 10 + random.nextInt(100);
        submitOrder(product, buyPrice, buyVol, BookSide.BUY);

        // Place Sell Order
        double sellPrice = basePrice * (1 + (spread / 2));
        int sellVol = 10 + random.nextInt(100);
        submitOrder(product, sellPrice, sellVol, BookSide.SELL);
    }
}
