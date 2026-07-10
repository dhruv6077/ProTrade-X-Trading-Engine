package exchange.dispatch;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.TimeoutException;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.EventHandlerGroup;
import com.lmax.disruptor.dsl.ProducerType;
import exchange.core.AffinityThreadFactory;
import exchange.core.FailSafeDisruptorExceptionHandler;
import exchange.model.ExchangeEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public final class InMemoryEventDispatcher implements EventDispatcher {
    public static final int DEFAULT_RING_BUFFER_SIZE = 65_536;
    public static final int DEFAULT_HISTORY_SIZE = 65_536;

    private static final Logger logger = LoggerFactory.getLogger(InMemoryEventDispatcher.class);

    private final RingBufferEvent[] history;
    private final AtomicLong historyCursor = new AtomicLong();
    private final ListenerFanoutHandler listenerFanoutHandler = new ListenerFanoutHandler();
    private final DelegatingTrailingHandler delegatingTrailingHandler = new DelegatingTrailingHandler();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Disruptor<RingBufferEvent> disruptor;

    public InMemoryEventDispatcher() {
        this(List.of(), DEFAULT_RING_BUFFER_SIZE);
    }

    public InMemoryEventDispatcher(List<? extends EventHandler<RingBufferEvent>> primaryHandlers) {
        this(primaryHandlers, DEFAULT_RING_BUFFER_SIZE);
    }

    public InMemoryEventDispatcher(List<? extends EventHandler<RingBufferEvent>> primaryHandlers, int ringBufferSize) {
        if (Integer.bitCount(ringBufferSize) != 1) {
            throw new IllegalArgumentException("Ring buffer size must be a power of two");
        }
        this.history = new RingBufferEvent[DEFAULT_HISTORY_SIZE];
        for (int i = 0; i < history.length; i++) {
            history[i] = new RingBufferEvent();
        }
        AffinityThreadFactory threadFactory = new AffinityThreadFactory("exchange-event-disruptor");

        disruptor = new Disruptor<>(ExchangeEventFactory.INSTANCE, ringBufferSize, threadFactory, ProducerType.MULTI,
                new YieldingWaitStrategy());
        disruptor.setDefaultExceptionHandler(new FailSafeDisruptorExceptionHandler<>("exchange-event-dispatcher"));

        ArrayList<EventHandler<RingBufferEvent>> handlers = new ArrayList<>(primaryHandlers.size() + 1);
        handlers.addAll(primaryHandlers);
        handlers.add(listenerFanoutHandler);
        EventHandlerGroup<RingBufferEvent> primaryGroup = disruptor.handleEventsWith(handlerArray(handlers));
        primaryGroup.then(delegatingTrailingHandler);
        disruptor.start();
    }

    @Override
    public void publish(List<ExchangeEvent> events) {
        if (closed.get()) {
            throw new IllegalStateException("Event dispatcher is closed");
        }
        if (events.isEmpty()) {
            return;
        }
        int batchSize = events.size();
        for (int index = 0; index < batchSize; index++) {
            ExchangeEvent event = events.get(index);
            appendHistory(event);
            disruptor.publishEvent((slot, sequence, publishedEvent, batchIndex, publishedBatchSize) ->
                            slot.copyFrom(publishedEvent, batchIndex, publishedBatchSize),
                    event, index, batchSize);
        }
    }

    @Override
    public void publish(MutableEventBatch events) {
        if (closed.get()) {
            throw new IllegalStateException("Event dispatcher is closed");
        }
        if (events.isEmpty()) {
            return;
        }
        events.updateBatchSize();
        int batchSize = events.size();
        for (int index = 0; index < batchSize; index++) {
            RingBufferEvent event = events.get(index);
            appendHistory(event);
            disruptor.publishEvent((slot, sequence, publishedEvent, batchIndex, publishedBatchSize) ->
                            slot.copyFrom(publishedEvent, batchIndex, publishedBatchSize),
                    event, index, batchSize);
        }
    }

    @Override
    public void addListener(EventListener listener) {
        listenerFanoutHandler.addListener(listener);
    }

    @Override
    public void removeListener(EventListener listener) {
        listenerFanoutHandler.removeListener(listener);
    }

    public void addRingBufferListener(RingBufferEventListener listener) {
        listenerFanoutHandler.addRingBufferListener(listener);
    }

    public void removeRingBufferListener(RingBufferEventListener listener) {
        listenerFanoutHandler.removeRingBufferListener(listener);
    }

    public void attachTrailingHandler(EventHandler<RingBufferEvent> trailingHandler) {
        delegatingTrailingHandler.addDelegate(trailingHandler);
    }

    public List<ExchangeEvent> events() {
        long cursor = historyCursor.get();
        long available = Math.min(cursor, history.length);
        ArrayList<ExchangeEvent> snapshot = new ArrayList<>((int) available);
        long start = cursor - available;
        for (long sequence = start; sequence < cursor; sequence++) {
            RingBufferEvent event = history[(int) (sequence % history.length)];
            if (event != null) {
                snapshot.add(event.toImmutableEvent());
            }
        }
        return snapshot;
    }

    private void appendHistory(ExchangeEvent event) {
        long sequence = historyCursor.getAndIncrement();
        history[(int) (sequence % history.length)].copyFrom(event, 0, 1);
    }

    private void appendHistory(RingBufferEvent event) {
        long sequence = historyCursor.getAndIncrement();
        history[(int) (sequence % history.length)].copyFrom(event, 0, 1);
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        try {
            disruptor.shutdown(5, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            logger.warn("Disruptor did not drain within timeout; halting event processor");
            disruptor.halt();
        }
        delegatingTrailingHandler.close();
    }

    private static final class ListenerFanoutHandler implements EventHandler<RingBufferEvent> {
        private volatile EventListener[] listeners = new EventListener[0];
        private volatile RingBufferEventListener[] ringBufferListeners = new RingBufferEventListener[0];

        private void addListener(EventListener listener) {
            Objects.requireNonNull(listener);
            synchronized (this) {
                EventListener[] current = listeners;
                EventListener[] updated = new EventListener[current.length + 1];
                System.arraycopy(current, 0, updated, 0, current.length);
                updated[current.length] = listener;
                listeners = updated;
            }
        }

        private void removeListener(EventListener listener) {
            synchronized (this) {
                listeners = removeListener(listeners, listener);
            }
        }

        private void addRingBufferListener(RingBufferEventListener listener) {
            Objects.requireNonNull(listener);
            synchronized (this) {
                RingBufferEventListener[] current = ringBufferListeners;
                RingBufferEventListener[] updated = new RingBufferEventListener[current.length + 1];
                System.arraycopy(current, 0, updated, 0, current.length);
                updated[current.length] = listener;
                ringBufferListeners = updated;
            }
        }

        private void removeRingBufferListener(RingBufferEventListener listener) {
            synchronized (this) {
                ringBufferListeners = removeListener(ringBufferListeners, listener);
            }
        }

        @Override
        public void onEvent(RingBufferEvent event, long sequence, boolean endOfBatch) {
            RingBufferEventListener[] currentRingBufferListeners = ringBufferListeners;
            for (int i = 0; i < currentRingBufferListeners.length; i++) {
                currentRingBufferListeners[i].onRingBufferEvent(event, sequence, endOfBatch);
            }

            EventListener[] currentListeners = listeners;
            if (currentListeners.length > 0) {
                List<ExchangeEvent> immutableEvents = List.of(event.toImmutableEvent());
                for (int i = 0; i < currentListeners.length; i++) {
                    currentListeners[i].onEvents(immutableEvents);
                }
            }
        }

        private static EventListener[] removeListener(EventListener[] current, EventListener listener) {
            int index = -1;
            for (int i = 0; i < current.length; i++) {
                if (current[i] == listener) {
                    index = i;
                    break;
                }
            }
            if (index < 0) {
                return current;
            }
            EventListener[] updated = new EventListener[current.length - 1];
            System.arraycopy(current, 0, updated, 0, index);
            System.arraycopy(current, index + 1, updated, index, current.length - index - 1);
            return updated;
        }

        private static RingBufferEventListener[] removeListener(RingBufferEventListener[] current,
                RingBufferEventListener listener) {
            int index = -1;
            for (int i = 0; i < current.length; i++) {
                if (current[i] == listener) {
                    index = i;
                    break;
                }
            }
            if (index < 0) {
                return current;
            }
            RingBufferEventListener[] updated = new RingBufferEventListener[current.length - 1];
            System.arraycopy(current, 0, updated, 0, index);
            System.arraycopy(current, index + 1, updated, index, current.length - index - 1);
            return updated;
        }
    }

    private static final class DelegatingTrailingHandler implements EventHandler<RingBufferEvent> {
        private volatile EventHandler<RingBufferEvent>[] delegates = emptyHandlerArray();

        private void addDelegate(EventHandler<RingBufferEvent> delegate) {
            Objects.requireNonNull(delegate);
            synchronized (this) {
                EventHandler<RingBufferEvent>[] current = delegates;
                EventHandler<RingBufferEvent>[] updated = handlerArray(current.length + 1);
                System.arraycopy(current, 0, updated, 0, current.length);
                updated[current.length] = delegate;
                delegates = updated;
            }
        }

        @Override
        public void onEvent(RingBufferEvent event, long sequence, boolean endOfBatch) throws Exception {
            EventHandler<RingBufferEvent>[] current = delegates;
            for (int i = 0; i < current.length; i++) {
                current[i].onEvent(event, sequence, endOfBatch);
            }
        }

        private void close() {
            EventHandler<RingBufferEvent>[] current = delegates;
            for (int i = 0; i < current.length; i++) {
                if (current[i] instanceof AutoCloseable autoCloseable) {
                    try {
                        autoCloseable.close();
                    } catch (Exception e) {
                        logger.warn("Failed to close trailing event handler cleanly", e);
                    }
                }
            }
        }
    }

    private static EventHandler<RingBufferEvent>[] handlerArray(List<EventHandler<RingBufferEvent>> handlers) {
        EventHandler<RingBufferEvent>[] array = handlerArray(handlers.size());
        for (int i = 0; i < handlers.size(); i++) {
            array[i] = handlers.get(i);
        }
        return array;
    }

    private static EventHandler<RingBufferEvent>[] emptyHandlerArray() {
        return handlerArray(0);
    }

    @SuppressWarnings("unchecked")
    private static EventHandler<RingBufferEvent>[] handlerArray(int size) {
        return (EventHandler<RingBufferEvent>[]) new EventHandler<?>[size];
    }
}
