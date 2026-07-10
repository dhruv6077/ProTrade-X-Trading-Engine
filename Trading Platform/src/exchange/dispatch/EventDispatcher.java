package exchange.dispatch;

import exchange.model.ExchangeEvent;

import java.util.List;

public interface EventDispatcher extends AutoCloseable {
    void publish(List<ExchangeEvent> events);

    default void publish(MutableEventBatch events) {
        for (int i = 0; i < events.size(); i++) {
            publish(List.of(events.get(i).toImmutableEvent()));
        }
    }

    void addListener(EventListener listener);

    void removeListener(EventListener listener);

    @Override
    void close();
}
