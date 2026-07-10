package sim;

import Tradable.BookSide;
import Tradable.TradableDTO;
import User.User;
import User.UserManager;
import exchange.core.ExchangeRuntime;
import exchange.model.CancelOrderCommand;
import exchange.model.ExchangeEvent;
import exchange.model.OrderAccepted;
import exchange.model.OrderExecuted;
import exchange.model.OrderRejected;
import exchange.model.OrderType;
import exchange.model.SelfTradePreventionMode;
import exchange.model.Side;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import Price.Price;
import Price.PriceFactory;

import java.util.List;
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

    protected void submitOrder(String product, long priceCents, int volume, BookSide side) {
        try {
            Price p = PriceFactory.makePrice(priceCents);
            String orderId = botId + product + p + System.nanoTime();
            List<ExchangeEvent> events = ExchangeRuntime.getInstance().gateway().submitNewOrder(
                    orderId,
                    botId,
                    product,
                    toExchangeSide(side),
                    OrderType.LIMIT,
                    p,
                    volume,
                    SelfTradePreventionMode.CANCEL_NEWEST);
            if (wasRejected(events)) {
                logger.debug("{} order rejected by gateway: {}", botId, orderId);
                return;
            }
            TradableDTO dto = dtoFromEvents(orderId, product, p, volume, side, events);
            user.addTradable(dto);

            logger.debug("{} placed {} {} @ {}", botId, side, volume, p);
        } catch (Exception e) {
            logger.warn("{} failed to place order: {}", botId, e.getMessage());
        }
    }

    protected void cancelOrder(TradableDTO dto) {
        ExchangeRuntime.getInstance().gateway().process(
                new CancelOrderCommand(0, dto.id, botId, dto.product));
    }

    private TradableDTO dtoFromEvents(
            String orderId,
            String product,
            Price price,
            int originalVolume,
            BookSide side,
            List<ExchangeEvent> events) {
        int leavesQty = originalVolume;
        int cumQty = 0;
        for (ExchangeEvent event : events) {
            if (event instanceof OrderAccepted accepted && accepted.orderId().equals(orderId)) {
                leavesQty = accepted.order().leavesQty();
                cumQty = accepted.order().cumQty();
            } else if (event instanceof OrderExecuted executed && executed.orderId().equals(orderId)) {
                leavesQty = executed.leavesQty();
                cumQty = executed.cumQty();
            }
        }
        return new TradableDTO(orderId, botId, product, price, side, originalVolume, leavesQty, 0, cumQty);
    }

    private boolean wasRejected(List<ExchangeEvent> events) {
        return events.stream().anyMatch(OrderRejected.class::isInstance);
    }

    private Side toExchangeSide(BookSide side) {
        return side == BookSide.BUY ? Side.BUY : Side.SELL;
    }
}
