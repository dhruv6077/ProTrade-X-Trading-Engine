package exchange.dispatch;

import com.lmax.disruptor.EventFactory;

public final class ExchangeEventFactory implements EventFactory<RingBufferEvent> {
    public static final ExchangeEventFactory INSTANCE = new ExchangeEventFactory();

    private ExchangeEventFactory() {
    }

    @Override
    public RingBufferEvent newInstance() {
        return new RingBufferEvent();
    }
}
