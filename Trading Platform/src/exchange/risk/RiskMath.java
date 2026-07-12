package exchange.risk;

import exchange.model.Side;

/** Allocation-free, saturating arithmetic for the pre-trade risk boundary. */
final class RiskMath {
    private RiskMath() {
    }

    static long multiplyPositiveOrMax(long left, long right) {
        if (left <= 0L || right <= 0L) {
            return 0L;
        }
        return left > Long.MAX_VALUE / right ? Long.MAX_VALUE : left * right;
    }

    static boolean exceedsPositionLimit(long currentPosition, Side side, int quantity, int maxPosition) {
        long limit = maxPosition;
        if (currentPosition > limit || currentPosition < -limit) {
            return true;
        }
        return side == Side.BUY
                ? currentPosition > limit - quantity
                : currentPosition < -limit + quantity;
    }
}
