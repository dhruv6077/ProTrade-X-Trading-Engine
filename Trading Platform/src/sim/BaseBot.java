package sim;

import Tradable.BookSide;
import Tradable.Order;
import Tradable.TradableDTO;
import User.ProductManager;
import User.User;
import User.UserManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

/**
 * Abstract base class for all trading bots.
 */
public abstract class BaseBot implements Runnable {
    protected final Logger logger = LoggerFactory.getLogger(getClass());
    protected final String botId;
    protected final User user;
    protected final Random random = new Random();
    protected volatile boolean running = true;
    protected final long sleepInterval;

    public BaseBot(String botId, long sleepInterval) {
        this.botId = botId;
        this.sleepInterval = sleepInterval;

        // Ensure user exists
        try {
            if (UserManager.getInstance().getUser(botId) == null) {
                UserManager.getInstance().init(new String[] { botId });
            }
            this.user = UserManager.getInstance().getUser(botId);
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize bot user: " + botId, e);
        }
    }

    public void stop() {
        running = false;
    }

    public void reset() {
        running = true;
    }

    @Override
    public void run() {
        logger.info("Bot {} started", botId);
        while (running) {
            try {
                executeStrategy();
                Thread.sleep(sleepInterval + random.nextInt(500)); // Add some jitter
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.error("Bot {} encountered error", botId, e);
            }
        }
        logger.info("Bot {} stopped", botId);
    }

    protected abstract void executeStrategy() throws Exception;

    protected void submitOrder(String product, double price, int volume, BookSide side) {
        try {
            // Convert double price to Price object logic is handled inside Order or
            // ProductManager
            // For now, we need to match the existing API which expects Price objects
            // But wait, the existing API is a bit complex with PriceFactory.
            // Let's simplify: We'll use the existing ProductManager.addTradable

            // We need to construct a Price object.
            // Since we don't have direct access to PriceFactory easily without imports,
            // let's assume we can get it or use a helper.
            // Actually, let's look at how TradingSim did it.
            // It used PriceFactory.makePrice((int) (price * 100));

            Price.Price p = Price.PriceFactory.makePrice((int) Math.round(price * 100));
            Order order = new Order(botId, product, p, volume, side);

            TradableDTO dto = ProductManager.getInstance().addTradable(order);
            user.addTradable(dto);

            logger.debug("{} placed {} {} @ {}", botId, side, volume, price);
        } catch (Exception e) {
            logger.warn("{} failed to place order: {}", botId, e.getMessage());
        }
    }
}
