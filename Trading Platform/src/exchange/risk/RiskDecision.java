package exchange.risk;

import exchange.model.RejectReason;

public record RiskDecision(boolean accepted, RejectReason rejectReason, String message) {
    private static final RiskDecision ACCEPTED = new RiskDecision(true, null, "");
    public static final RiskDecision GLOBAL_KILL_SWITCH =
            new RiskDecision(false, RejectReason.RISK_KILL_SWITCH, "Global kill switch is enabled");
    public static final RiskDecision CLIENT_KILL_SWITCH =
            new RiskDecision(false, RejectReason.RISK_KILL_SWITCH, "Client kill switch is enabled");
    public static final RiskDecision MARKET_REQUIRES_REFERENCE =
            new RiskDecision(false, RejectReason.RISK_BUYING_POWER,
                    "Market order requires a bounded reference price");
    public static final RiskDecision ORDER_NOTIONAL_LIMIT =
            new RiskDecision(false, RejectReason.RISK_NOTIONAL_LIMIT, "Order notional exceeds risk limit");
    public static final RiskDecision PROJECTED_POSITION_LIMIT =
            new RiskDecision(false, RejectReason.RISK_POSITION_LIMIT, "Projected position exceeds limit");
    public static final RiskDecision INSUFFICIENT_AVAILABLE_CASH =
            new RiskDecision(false, RejectReason.RISK_BUYING_POWER, "Insufficient available cash");
    public static final RiskDecision INSUFFICIENT_POSITION =
            new RiskDecision(false, RejectReason.RISK_POSITION_LIMIT, "Insufficient position to sell");
    public static final RiskDecision PRICE_BAND =
            new RiskDecision(false, RejectReason.RISK_PRICE_BAND, "Order price is outside the allowed price band");
    public static final RiskDecision WOULD_SELF_TRADE =
            new RiskDecision(false, RejectReason.WOULD_SELF_TRADE,
                    "Order would trade against the client's own resting liquidity");

    public static RiskDecision accept() {
        return ACCEPTED;
    }

    public static RiskDecision reject(RejectReason reason, String message) {
        return new RiskDecision(false, reason, message);
    }
}
