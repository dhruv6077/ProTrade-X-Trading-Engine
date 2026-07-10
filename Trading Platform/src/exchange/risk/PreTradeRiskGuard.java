package exchange.risk;

import Price.Price;
import exchange.model.MutableOrderCommand;
import exchange.model.NewOrderCommand;
import exchange.model.OrderCommand;
import exchange.model.OrderType;
import exchange.model.Side;

/**
 * Stateless, allocation-light pre-trade guard for the ingress path.
 *
 * <p>This guard deliberately works on primitive values extracted from the
 * incoming command. It performs checks that do not mutate balances or order
 * books: max notional, optional price-band validation, and optional hard
 * self-cross rejection.</p>
 */
public final class PreTradeRiskGuard {
    public RiskDecision check(
            OrderCommand command,
            RiskProfile profile,
            long referencePriceCents,
            boolean wouldSelfTrade,
            boolean hardRejectSelfTrade) {
        if (!(command instanceof NewOrderCommand) && !(command instanceof MutableOrderCommand)) {
            return RiskDecision.accept();
        }

        Side side = side(command);
        OrderType orderType = orderType(command);
        int quantity = quantity(command);
        Price price = price(command);

        if (hardRejectSelfTrade && wouldSelfTrade) {
            return RiskDecision.WOULD_SELF_TRADE;
        }

        long unitPriceCents = price == null ? -1L : price.getCents();
        if (unitPriceCents > 0L) {
            long notionalCents = Math.multiplyExact(unitPriceCents, quantity);
            if (notionalCents > profile.maxOrderNotionalCents()) {
                return RiskDecision.ORDER_NOTIONAL_LIMIT;
            }
            if (orderType != OrderType.MARKET
                    && referencePriceCents > 0L
                    && outsidePriceBand(side, unitPriceCents, referencePriceCents,
                    profile.maxPriceDeviationBps())) {
                return RiskDecision.PRICE_BAND;
            }
        }

        return RiskDecision.accept();
    }

    private static boolean outsidePriceBand(Side side, long priceCents, long referenceCents, int maxDeviationBps) {
        if (maxDeviationBps <= 0) {
            return false;
        }
        long upper = Math.multiplyExact(referenceCents, 10_000L + maxDeviationBps) / 10_000L;
        long lower = Math.multiplyExact(referenceCents, Math.max(0L, 10_000L - maxDeviationBps)) / 10_000L;
        return side == Side.BUY ? priceCents > upper : priceCents < lower;
    }

    private static Side side(OrderCommand command) {
        return command instanceof NewOrderCommand newOrder ? newOrder.side() : ((MutableOrderCommand) command).side();
    }

    private static OrderType orderType(OrderCommand command) {
        return command instanceof NewOrderCommand newOrder
                ? newOrder.orderType()
                : ((MutableOrderCommand) command).orderType();
    }

    private static int quantity(OrderCommand command) {
        return command instanceof NewOrderCommand newOrder
                ? newOrder.quantity()
                : ((MutableOrderCommand) command).quantity();
    }

    private static Price price(OrderCommand command) {
        return command instanceof NewOrderCommand newOrder ? newOrder.price() : ((MutableOrderCommand) command).price();
    }
}
