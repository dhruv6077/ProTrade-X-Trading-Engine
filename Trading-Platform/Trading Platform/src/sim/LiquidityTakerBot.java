package sim;

import Tradable.BookSide;
import Tradable.TradableDTO;
import User.ProductManager;

import java.util.List;

/**
 * Liquidity Taker Bot that aggressively trades.
 * Simulates retail traders or aggressive algos.
 */
public class LiquidityTakerBot extends BaseBot {
    private final List<String> products;

    public LiquidityTakerBot(String botId, List<String> products, long sleepInterval) {
        super(botId, sleepInterval);
        this.products = products;
    }

    @Override
    protected void executeStrategy() throws Exception {
        // 10% chance to cancel existing orders
        if (random.nextDouble() < 0.1 && user.hasTradableWithRemainingQty()) {
            TradableDTO dto = user.getTradableWithRemainingQty();
            ProductManager.getInstance().cancel(dto);
            logger.debug("{} cancelled order {}", botId, dto.id);
            return;
        }

        String product = products.get(random.nextInt(products.size()));
        BookSide side = random.nextBoolean() ? BookSide.BUY : BookSide.SELL;

        // Aggressive pricing
        double basePrice = 150.0; // Simplified
        double price = side == BookSide.BUY ? basePrice * 1.02 : basePrice * 0.98; // Cross the spread
        int volume = 5 + random.nextInt(50);

        submitOrder(product, price, volume, side);
    }
}
