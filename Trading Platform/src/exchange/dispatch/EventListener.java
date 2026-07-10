package exchange.dispatch;

import exchange.model.ExchangeEvent;

import java.util.List;

public interface EventListener {
    void onEvents(List<ExchangeEvent> events);
}
