package exchange.risk;

public record RiskProfile(
        long maxOrderNotionalCents,
        int maxPosition,
        long buyingPowerCents,
        boolean killSwitchEnabled,
        int maxPriceDeviationBps) {
    public static final int DEFAULT_MAX_PRICE_DEVIATION_BPS = 10_000;

    public RiskProfile(
            long maxOrderNotionalCents,
            int maxPosition,
            long buyingPowerCents,
            boolean killSwitchEnabled) {
        this(maxOrderNotionalCents, maxPosition, buyingPowerCents, killSwitchEnabled,
                DEFAULT_MAX_PRICE_DEVIATION_BPS);
    }
}
