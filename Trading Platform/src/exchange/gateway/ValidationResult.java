package exchange.gateway;

import exchange.model.RejectReason;

public record ValidationResult(boolean accepted, RejectReason rejectReason, String message) {
    private static final ValidationResult ACCEPTED = new ValidationResult(true, null, "");

    public static ValidationResult accept() {
        return ACCEPTED;
    }

    public static ValidationResult reject(RejectReason reason, String message) {
        return new ValidationResult(false, reason, message);
    }
}
