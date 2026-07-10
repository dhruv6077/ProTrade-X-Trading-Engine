package exchange.journal;

import exchange.model.OrderCommand;

import java.util.List;

public interface CommandJournal extends AutoCloseable {
    void append(OrderCommand command);

    List<OrderCommand> replay();

    long size();

    long totalAppended();

    @Override
    void close();
}
