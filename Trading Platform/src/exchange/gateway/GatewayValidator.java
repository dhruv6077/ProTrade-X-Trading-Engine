package exchange.gateway;

import exchange.core.SymbolDictionary;
import exchange.model.CancelOrderCommand;
import exchange.model.AdminCommand;
import exchange.model.ModifyOrderCommand;
import exchange.model.MutableAdminCommand;
import exchange.model.MutableCancelOrderCommand;
import exchange.model.MutableModifyOrderCommand;
import exchange.model.MutableOrderCommand;
import exchange.model.NewOrderCommand;
import exchange.model.OrderCommand;
import exchange.model.OrderType;
import exchange.model.RejectReason;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public final class GatewayValidator {
    private final Set<String> symbols;
    private final SymbolDictionary symbolDictionary;
    private final int tickSizeCents;
    private final int minQuantity;
    private final int maxQuantity;

    public GatewayValidator(Set<String> symbols, int tickSizeCents, int minQuantity, int maxQuantity) {
        this.symbols = ConcurrentHashMap.newKeySet();
        this.symbolDictionary = new SymbolDictionary();
        this.symbols.addAll(symbols);
        for (String symbol : symbols) {
            this.symbolDictionary.register(symbol);
        }
        this.tickSizeCents = tickSizeCents;
        this.minQuantity = minQuantity;
        this.maxQuantity = maxQuantity;
    }

    public int addSymbol(String symbol) {
        if (symbol != null) {
            symbols.add(symbol);
            return symbolDictionary.register(symbol);
        }
        return 0;
    }

    public int symbolId(String symbol) {
        return symbolDictionary.idFor(symbol);
    }

    public String symbolFor(int symbolId) {
        return symbolDictionary.symbolFor(symbolId);
    }

    public ValidationResult validate(OrderCommand command) {
        if (!isValidClientId(command.clientId())) {
            return ValidationResult.reject(RejectReason.INVALID_CLIENT, "Invalid client_id");
        }
        if (!isValidSymbol(command.symbol()) || !symbolDictionary.contains(command.symbol())) {
            return ValidationResult.reject(RejectReason.INVALID_SYMBOL, "Invalid or unknown symbol");
        }
        if (!isValidOrderId(command.orderId())) {
            return ValidationResult.reject(RejectReason.INVALID_ORDER_ID, "Invalid order_id");
        }
        return switch (command) {
            case NewOrderCommand newOrder -> validateNewOrder(newOrder);
            case MutableOrderCommand newOrder -> validateNewOrder(newOrder);
            case AdminCommand admin -> validateAdmin(admin);
            case MutableAdminCommand admin -> validateAdmin(admin);
            case CancelOrderCommand ignored -> ValidationResult.accept();
            case MutableCancelOrderCommand ignored -> ValidationResult.accept();
            case ModifyOrderCommand modify -> validateModify(modify);
            case MutableModifyOrderCommand modify -> validateModify(modify);
            default -> ValidationResult.reject(RejectReason.INVALID_ORDER_TYPE, "Unsupported command type");
        };
    }

    private ValidationResult validateAdmin(AdminCommand command) {
        if (command.operation() == null) {
            return ValidationResult.reject(RejectReason.INVALID_ORDER_TYPE, "Admin operation is required");
        }
        return ValidationResult.accept();
    }

    private ValidationResult validateAdmin(MutableAdminCommand command) {
        if (command.operation() == null) {
            return ValidationResult.reject(RejectReason.INVALID_ORDER_TYPE, "Admin operation is required");
        }
        return ValidationResult.accept();
    }

    private ValidationResult validateNewOrder(NewOrderCommand command) {
        return validateNewOrder(command.side(), command.orderType(), command.quantity(), command.price());
    }

    private ValidationResult validateNewOrder(MutableOrderCommand command) {
        return validateNewOrder(command.side(), command.orderType(), command.quantity(), command.price());
    }

    private ValidationResult validateNewOrder(exchange.model.Side side, OrderType orderType, int quantity,
            Price.Price price) {
        if (side == null || orderType == null) {
            return ValidationResult.reject(RejectReason.INVALID_ORDER_TYPE, "Side and order_type are required");
        }
        if (quantity < minQuantity || quantity > maxQuantity) {
            return ValidationResult.reject(RejectReason.INVALID_QUANTITY, "Quantity outside allowed range");
        }
        if (orderType == OrderType.STOP || orderType == OrderType.STOP_LIMIT) {
            return ValidationResult.reject(RejectReason.UNSUPPORTED_ORDER_TYPE, "Stop orders require trigger management");
        }
        if (orderType != OrderType.MARKET) {
            return validatePrice(price);
        }
        return ValidationResult.accept();
    }

    private ValidationResult validateModify(ModifyOrderCommand command) {
        if (command.newQuantity() < minQuantity || command.newQuantity() > maxQuantity) {
            return ValidationResult.reject(RejectReason.INVALID_QUANTITY, "Quantity outside allowed range");
        }
        return validatePrice(command.newPrice());
    }

    private ValidationResult validateModify(MutableModifyOrderCommand command) {
        if (command.newQuantity() < minQuantity || command.newQuantity() > maxQuantity) {
            return ValidationResult.reject(RejectReason.INVALID_QUANTITY, "Quantity outside allowed range");
        }
        return validatePrice(command.newPrice());
    }

    private ValidationResult validatePrice(Price.Price price) {
        if (price == null || price.getCents() <= 0 || price.getCents() % tickSizeCents != 0) {
            return ValidationResult.reject(RejectReason.INVALID_PRICE, "Price must be positive and match tick size");
        }
        return ValidationResult.accept();
    }

    private static boolean isValidClientId(String value) {
        int length = value == null ? 0 : value.length();
        if (length < 3 || length > 20) {
            return false;
        }
        for (int i = 0; i < length; i++) {
            char c = value.charAt(i);
            if (!isAsciiAlphaNumeric(c) && c != '_') {
                return false;
            }
        }
        return true;
    }

    private static boolean isValidOrderId(String value) {
        int length = value == null ? 0 : value.length();
        if (length < 1 || length > 96) {
            return false;
        }
        for (int i = 0; i < length; i++) {
            char c = value.charAt(i);
            if (!isAsciiAlphaNumeric(c) && c != '_' && c != '.' && c != '$' && c != ':' && c != '-') {
                return false;
            }
        }
        return true;
    }

    private static boolean isValidSymbol(String value) {
        int length = value == null ? 0 : value.length();
        if (length < 1 || length > 5) {
            return false;
        }
        for (int i = 0; i < length; i++) {
            char c = value.charAt(i);
            if (c < 'A' || c > 'Z') {
                return false;
            }
        }
        return true;
    }

    private static boolean isAsciiAlphaNumeric(char c) {
        return (c >= 'A' && c <= 'Z')
                || (c >= 'a' && c <= 'z')
                || (c >= '0' && c <= '9');
    }
}
