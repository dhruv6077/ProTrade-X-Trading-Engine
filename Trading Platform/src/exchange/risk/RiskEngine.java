package exchange.risk;

import exchange.dispatch.RingBufferEvent;
import exchange.model.ExchangeEvent;
import exchange.model.NewOrderCommand;
import exchange.model.OrderCommand;

import java.util.List;

public interface RiskEngine {
    RiskDecision check(OrderCommand command);

    void applyFill(NewOrderCommand aggressiveOrder, int signedQuantity, long notionalCents);

    default void releaseReservation(String orderId) {
    }

    default void onEvents(List<ExchangeEvent> events) {
    }

    default void onEvent(RingBufferEvent event) {
        onEvents(List.of(event.toImmutableEvent()));
    }
}
