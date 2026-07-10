package exchange.model;

import Price.Price;

import java.time.Instant;

public record NewOrderCommand(
        long sequenceNumber,
        Instant inboundTimestamp,
        String orderId,
        String clientId,
        String symbol,
        int symbolId,
        Side side,
        OrderType orderType,
        Price price,
        int quantity,
        SelfTradePreventionMode stpMode,
        long ingressTimeNs) implements OrderCommand {

    public NewOrderCommand(
            long sequenceNumber,
            Instant inboundTimestamp,
            String orderId,
            String clientId,
            String symbol,
            Side side,
            OrderType orderType,
            Price price,
            int quantity,
            SelfTradePreventionMode stpMode,
            long ingressTimeNs) {
        this(sequenceNumber, inboundTimestamp, orderId, clientId, symbol, 0, side, orderType, price, quantity, stpMode,
                ingressTimeNs);
    }

    public NewOrderCommand(
            long sequenceNumber,
            Instant inboundTimestamp,
            String orderId,
            String clientId,
            String symbol,
            Side side,
            OrderType orderType,
            Price price,
            int quantity,
            SelfTradePreventionMode stpMode) {
        this(sequenceNumber, inboundTimestamp, orderId, clientId, symbol, side, orderType, price, quantity, stpMode,
                0L);
    }

    public NewOrderCommand(
            long sequenceNumber,
            Instant inboundTimestamp,
            String orderId,
            String clientId,
            String symbol,
            int symbolId,
            Side side,
            OrderType orderType,
            Price price,
            int quantity,
            SelfTradePreventionMode stpMode) {
        this(sequenceNumber, inboundTimestamp, orderId, clientId, symbol, symbolId, side, orderType, price, quantity,
                stpMode, 0L);
    }

    public NewOrderCommand(
            long sequenceNumber,
            String orderId,
            String clientId,
            String symbol,
            Side side,
            OrderType orderType,
            Price price,
            int quantity,
            SelfTradePreventionMode stpMode) {
        this(sequenceNumber, Instant.EPOCH, orderId, clientId, symbol, 0, side, orderType, price, quantity, stpMode,
                0L);
    }

    public NewOrderCommand(
            long sequenceNumber,
            String orderId,
            String clientId,
            String symbol,
            int symbolId,
            Side side,
            OrderType orderType,
            Price price,
            int quantity,
            SelfTradePreventionMode stpMode) {
        this(sequenceNumber, Instant.EPOCH, orderId, clientId, symbol, symbolId, side, orderType, price, quantity,
                stpMode, 0L);
    }

    @Override
    public CommandType commandType() {
        return CommandType.NEW_ORDER;
    }

    @Override
    public NewOrderCommand withSequencing(long sequenceNumber, Instant inboundTimestamp) {
        return new NewOrderCommand(sequenceNumber, inboundTimestamp, orderId, clientId, symbol, symbolId, side,
                orderType, price, quantity, stpMode, ingressTimeNs);
    }

    public NewOrderCommand withSymbolId(int symbolId) {
        if (this.symbolId == symbolId) {
            return this;
        }
        return new NewOrderCommand(sequenceNumber, inboundTimestamp, orderId, clientId, symbol, symbolId, side,
                orderType, price, quantity, stpMode, ingressTimeNs);
    }
}
