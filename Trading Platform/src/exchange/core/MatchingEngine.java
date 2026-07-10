package exchange.core;

import exchange.model.ExchangeEvent;
import exchange.model.OrderCommand;
import exchange.dispatch.MutableEventBatch;

import java.util.List;

public interface MatchingEngine {
    List<ExchangeEvent> process(OrderCommand command);

    default MutableEventBatch processInto(OrderCommand command, MutableEventBatch batch) {
        batch.reset();
        batch.addAll(process(command));
        return batch;
    }
}
