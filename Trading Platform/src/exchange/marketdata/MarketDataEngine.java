package exchange.marketdata;

import Price.Price;
import com.lmax.disruptor.EventHandler;
import exchange.dispatch.EventListener;
import exchange.dispatch.RingBufferEvent;
import exchange.model.ExchangeEvent;
import exchange.model.OrderAccepted;
import exchange.model.OrderCancelled;
import exchange.model.OrderExecuted;
import exchange.model.OrderRestated;
import exchange.model.OrderState;
import exchange.model.OrderType;
import exchange.model.Side;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public final class MarketDataEngine implements EventHandler<RingBufferEvent>, EventListener, AutoCloseable {
    private static final int DEFAULT_BOOK_DEPTH = 10;
    private static final int DEFAULT_TAPE_LIMIT = 100;
    private static final int DEFAULT_CANDLE_LIMIT = 256;
    private static final int DEFAULT_ORDER_PROJECTION_POOL_SIZE = configuredOrderProjectionPoolSize();
    private static final int ORDER_PROJECTION_MAP_CAPACITY = mapCapacity(DEFAULT_ORDER_PROJECTION_POOL_SIZE);
    private static final long DEFAULT_SNAPSHOT_CONFLATION_NANOS = configuredSnapshotConflationNanos();

    private final Map<String, SymbolProjection> projections = new HashMap<>();
    private final ConcurrentHashMap<String, AtomicReference<L2Snapshot>> l2Snapshots = new ConcurrentHashMap<>();
    private final AtomicReference<List<TradeRecord>> tradeTapeSnapshot = new AtomicReference<>(List.of());
    private final AtomicReference<List<OhlcvCandle>> closedCandlesSnapshot = new AtomicReference<>(List.of());
    private final AtomicReference<List<CandleClosed>> candleEventsSnapshot = new AtomicReference<>(List.of());
    private final ArrayDeque<TradeRecord> tradeTape = new ArrayDeque<>(DEFAULT_TAPE_LIMIT);
    private final ArrayDeque<OhlcvCandle> closedCandles = new ArrayDeque<>(DEFAULT_CANDLE_LIMIT);
    private final ArrayDeque<CandleClosed> candleEvents = new ArrayDeque<>(DEFAULT_CANDLE_LIMIT);
    private final ArrayDeque<OrderProjection> orderProjectionPool = new ArrayDeque<>(DEFAULT_ORDER_PROJECTION_POOL_SIZE);
    private final Map<String, MutableCandle> currentCandles = new HashMap<>();
    private final Duration candleWindow;
    private final long snapshotConflationNanos;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private volatile MarketDataListener[] listeners = new MarketDataListener[0];
    private long lastTradeSequence = Long.MIN_VALUE;
    private long lastTradePriceCents;
    private int lastTradeQty;
    private String lastTradeOrderId;
    private String lastTradeContraOrderId;

    public MarketDataEngine() {
        this(Duration.ofMinutes(1));
    }

    public MarketDataEngine(Duration candleWindow) {
        this.candleWindow = candleWindow;
        this.snapshotConflationNanos = DEFAULT_SNAPSHOT_CONFLATION_NANOS;
        for (int i = 0; i < DEFAULT_ORDER_PROJECTION_POOL_SIZE; i++) {
            orderProjectionPool.addLast(new OrderProjection());
        }
    }

    private static int configuredOrderProjectionPoolSize() {
        String configured = System.getProperty("mdeOrderProjectionPoolSize");
        if (configured == null || configured.isBlank()) {
            configured = System.getenv("MDE_ORDER_PROJECTION_POOL_SIZE");
        }
        return configured == null || configured.isBlank() ? 16_384 : Integer.parseInt(configured);
    }

    private static int mapCapacity(int expectedEntries) {
        return Math.max(16, (int) Math.min(1 << 30, (long) (expectedEntries / 0.75d) + 1L));
    }

    private static long configuredSnapshotConflationNanos() {
        String configured = System.getProperty("mdeSnapshotIntervalMs");
        if (configured == null || configured.isBlank()) {
            configured = System.getenv("MDE_SNAPSHOT_INTERVAL_MS");
        }
        long millis = configured == null || configured.isBlank() ? 10L : Long.parseLong(configured);
        return Math.max(0L, millis) * 1_000_000L;
    }

    @Override
    public void onEvent(RingBufferEvent event, long sequence, boolean endOfBatch) {
        if (!closed.get()) {
            apply(event);
        }
    }

    @Override
    public void onEvents(List<ExchangeEvent> events) {
        if (closed.get()) {
            return;
        }
        for (ExchangeEvent event : events) {
            apply(event);
        }
    }

    public L2Snapshot l2Snapshot(String symbol, int depth) {
        SymbolProjection projection = projections.get(symbol);
        AtomicReference<L2Snapshot> reference = l2Snapshots.get(symbol);
        if (reference != null) {
            L2Snapshot snapshot = reference.get();
            if (snapshot != null && snapshot.asOf() != Instant.EPOCH
                    && (projection == null || !projection.isDirty())) {
                return snapshot.top(depth);
            }
        }

        if (projection == null) {
            return L2Snapshot.empty(symbol);
        }
        L2Snapshot snapshot = projection.snapshot(depth, projection.lastEventTimestamp());
        referenceFor(symbol).set(snapshot);
        return snapshot;
    }

    public List<TradeRecord> tradeTape() {
        return tradeTapeSnapshot.get();
    }

    public List<TradeRecord> tradeTape(String symbol, int limit) {
        List<TradeRecord> snapshot = tradeTapeSnapshot.get();
        if (snapshot.isEmpty()) {
            return List.of();
        }
        ArrayList<TradeRecord> filtered = new ArrayList<>(Math.min(limit, snapshot.size()));
        for (int i = snapshot.size() - 1; i >= 0 && filtered.size() < limit; i--) {
            TradeRecord trade = snapshot.get(i);
            if (trade.symbol().equals(symbol)) {
                filtered.add(trade);
            }
        }
        filtered.sort(Comparator.comparingLong(TradeRecord::sequenceNumber));
        return List.copyOf(filtered);
    }

    public List<OhlcvCandle> closedCandles() {
        return closedCandlesSnapshot.get();
    }

    public List<CandleClosed> candleClosedEvents() {
        return candleEventsSnapshot.get();
    }

    public void addListener(MarketDataListener listener) {
        synchronized (this) {
            MarketDataListener[] current = listeners;
            MarketDataListener[] updated = new MarketDataListener[current.length + 1];
            System.arraycopy(current, 0, updated, 0, current.length);
            updated[current.length] = listener;
            listeners = updated;
        }
    }

    public void removeListener(MarketDataListener listener) {
        synchronized (this) {
            MarketDataListener[] current = listeners;
            int index = -1;
            for (int i = 0; i < current.length; i++) {
                if (current[i] == listener) {
                    index = i;
                    break;
                }
            }
            if (index < 0) {
                return;
            }
            MarketDataListener[] updated = new MarketDataListener[current.length - 1];
            System.arraycopy(current, 0, updated, 0, index);
            System.arraycopy(current, index + 1, updated, index, current.length - index - 1);
            listeners = updated;
        }
    }

    private void apply(RingBufferEvent event) {
        SymbolProjection projection = projections.computeIfAbsent(event.getSymbol(), SymbolProjection::new);
        switch (event.getEventType()) {
            case ACCEPTED -> projection.accept(event);
            case EXECUTED -> {
                projection.execute(event);
                if (appendTrade(event)) {
                    updateCandle(event);
                }
            }
            case CANCELLED -> projection.cancel(event);
            case RESTATED -> projection.restate(event);
            default -> {
                return;
            }
        }
        projection.markDirty(event.getEventTimestamp());
        publishProjection(event.getSymbol(), projection, event.getEventTimestamp(),
                event.getEventType() == RingBufferEvent.EventType.EXECUTED);
    }

    private void apply(ExchangeEvent event) {
        SymbolProjection projection = projections.computeIfAbsent(event.symbol(), SymbolProjection::new);
        if (event instanceof OrderAccepted accepted) {
            projection.accept(accepted);
        } else if (event instanceof OrderExecuted executed) {
            projection.execute(executed);
            if (appendTrade(executed)) {
                updateCandle(executed);
            }
        } else if (event instanceof OrderCancelled cancelled) {
            projection.cancel(cancelled);
        } else if (event instanceof OrderRestated restated) {
            projection.restate(restated);
        } else {
            return;
        }
        projection.markDirty(event.eventTimestamp());
        publishProjection(event.symbol(), projection, event.eventTimestamp(), event instanceof OrderExecuted);
    }

    private void publishProjection(String symbol, SymbolProjection projection, Instant eventTimestamp, boolean tradeEvent) {
        MarketDataListener[] currentListeners = listeners;
        boolean hasSubscriber = false;
        for (int i = 0; i < currentListeners.length; i++) {
            if (currentListeners[i].hasSubscribers(symbol)) {
                hasSubscriber = true;
                break;
            }
        }

        if (!hasSubscriber) {
            return;
        }

        long nowNanos = System.nanoTime();
        if (!tradeEvent && !projection.shouldPublish(nowNanos, snapshotConflationNanos)) {
            return;
        }

        L2Snapshot snapshot = projection.snapshot(DEFAULT_BOOK_DEPTH, eventTimestamp);
        projection.markPublished(nowNanos);
        referenceFor(symbol).set(snapshot);

        List<TradeRecord> trades = null;
        for (int i = 0; i < currentListeners.length; i++) {
            MarketDataListener listener = currentListeners[i];
            if (!listener.hasSubscribers(snapshot.symbol())) {
                continue;
            }
            listener.onL2Snapshot(snapshot);
            if (tradeEvent) {
                if (trades == null) {
                    trades = tradeTape(symbol, DEFAULT_TAPE_LIMIT);
                }
                listener.onTradeTape(snapshot.symbol(), trades);
            }
        }
    }

    private AtomicReference<L2Snapshot> referenceFor(String symbol) {
        AtomicReference<L2Snapshot> reference = l2Snapshots.get(symbol);
        if (reference != null) {
            return reference;
        }
        return l2Snapshots.computeIfAbsent(symbol, ignored -> new AtomicReference<>(L2Snapshot.empty(symbol)));
    }

    private boolean appendTrade(RingBufferEvent execution) {
        if (isDuplicateTrade(execution.getSequenceNumber(), execution.getOrderId(), execution.getContraOrderId(),
                execution.getFillPrice().getCents(), execution.getFillQty())) {
            return false;
        }
        tradeTape.addLast(new TradeRecord(execution.getSequenceNumber(), execution.getSymbol(), execution.getFillPrice(),
                execution.getFillQty(), execution.getEventTimestamp(), execution.getSide()));
        trimTradeTape();
        tradeTapeSnapshot.set(List.copyOf(tradeTape));
        return true;
    }

    private boolean appendTrade(OrderExecuted execution) {
        if (isDuplicateTrade(execution.sequenceNumber(), execution.orderId(), execution.contraOrderId(),
                execution.fillPrice().getCents(), execution.fillQty())) {
            return false;
        }
        tradeTape.addLast(new TradeRecord(execution.sequenceNumber(), execution.symbol(), execution.fillPrice(),
                execution.fillQty(), execution.eventTimestamp(), execution.side()));
        trimTradeTape();
        tradeTapeSnapshot.set(List.copyOf(tradeTape));
        return true;
    }

    private boolean isDuplicateTrade(long sequenceNumber, String orderId, String contraOrderId, long priceCents,
            int fillQty) {
        boolean duplicate = sequenceNumber == lastTradeSequence
                && priceCents == lastTradePriceCents
                && fillQty == lastTradeQty
                && orderId.equals(lastTradeContraOrderId)
                && contraOrderId.equals(lastTradeOrderId);
        lastTradeSequence = sequenceNumber;
        lastTradePriceCents = priceCents;
        lastTradeQty = fillQty;
        lastTradeOrderId = orderId;
        lastTradeContraOrderId = contraOrderId;
        return duplicate;
    }

    private void trimTradeTape() {
        while (tradeTape.size() > DEFAULT_TAPE_LIMIT) {
            tradeTape.removeFirst();
        }
    }

    private void updateCandle(RingBufferEvent execution) {
        Instant windowStart = windowStart(execution.getEventTimestamp());
        MutableCandle candle = currentCandles.get(execution.getSymbol());
        if (candle != null && !candle.windowStart.equals(windowStart)) {
            closeCandle(candle);
            candle = null;
        }
        if (candle == null) {
            candle = new MutableCandle(execution.getSymbol(), windowStart, windowStart.plus(candleWindow),
                    execution.getFillPrice());
            currentCandles.put(execution.getSymbol(), candle);
        }
        candle.add(execution.getFillPrice(), execution.getFillQty());
    }

    private void updateCandle(OrderExecuted execution) {
        Instant windowStart = windowStart(execution.eventTimestamp());
        MutableCandle candle = currentCandles.get(execution.symbol());
        if (candle != null && !candle.windowStart.equals(windowStart)) {
            closeCandle(candle);
            candle = null;
        }
        if (candle == null) {
            candle = new MutableCandle(execution.symbol(), windowStart, windowStart.plus(candleWindow),
                    execution.fillPrice());
            currentCandles.put(execution.symbol(), candle);
        }
        candle.add(execution.fillPrice(), execution.fillQty());
    }

    private void closeCandle(MutableCandle candle) {
        OhlcvCandle closedCandle = candle.toClosed();
        closedCandles.addLast(closedCandle);
        candleEvents.addLast(new CandleClosed(closedCandle));
        while (closedCandles.size() > DEFAULT_CANDLE_LIMIT) {
            closedCandles.removeFirst();
        }
        while (candleEvents.size() > DEFAULT_CANDLE_LIMIT) {
            candleEvents.removeFirst();
        }
        closedCandlesSnapshot.set(List.copyOf(closedCandles));
        candleEventsSnapshot.set(List.copyOf(candleEvents));
    }

    private Instant windowStart(Instant timestamp) {
        long windowMillis = candleWindow.toMillis();
        long epochMillis = timestamp.toEpochMilli();
        return Instant.ofEpochMilli(epochMillis - (epochMillis % windowMillis));
    }

    @Override
    public void close() {
        closed.set(true);
    }

    private OrderProjection borrowProjection() {
        OrderProjection projection = orderProjectionPool.pollFirst();
        if (projection == null) {
            throw ProjectionPoolExhaustedException.INSTANCE;
        }
        return projection;
    }

    private void recycle(OrderProjection projection) {
        projection.clear();
        if (orderProjectionPool.size() < DEFAULT_ORDER_PROJECTION_POOL_SIZE) {
            orderProjectionPool.addLast(projection);
        }
    }

    private final class SymbolProjection {
        private final String symbol;
        private final NavigableMap<Price, Long> bids = new TreeMap<>();
        private final NavigableMap<Price, Long> asks = new TreeMap<>();
        private final Map<String, OrderProjection> orders = new HashMap<>(ORDER_PROJECTION_MAP_CAPACITY);
        private Instant lastEventTimestamp = Instant.EPOCH;
        private boolean dirty;
        private long lastSnapshotNanos;

        private SymbolProjection(String symbol) {
            this.symbol = symbol;
        }

        private void markDirty(Instant eventTimestamp) {
            lastEventTimestamp = eventTimestamp;
            dirty = true;
        }

        private Instant lastEventTimestamp() {
            return lastEventTimestamp;
        }

        private boolean isDirty() {
            return dirty;
        }

        private boolean shouldPublish(long nowNanos, long conflationNanos) {
            return dirty && (lastSnapshotNanos == 0L || nowNanos - lastSnapshotNanos >= conflationNanos);
        }

        private void markPublished(long nowNanos) {
            dirty = false;
            lastSnapshotNanos = nowNanos;
        }

        private void accept(RingBufferEvent accepted) {
            if (accepted.getOrderType() != OrderType.LIMIT || accepted.getPrice() == null || accepted.getLeavesQty() <= 0) {
                return;
            }
            putProjection(accepted.getOrderId(), accepted.getSide(), accepted.getPrice(), accepted.getLeavesQty());
        }

        private void accept(OrderAccepted accepted) {
            OrderState order = accepted.order();
            if (order.orderType() != OrderType.LIMIT || order.price() == null || order.leavesQty() <= 0) {
                return;
            }
            putProjection(order.orderId(), order.side(), order.price(), order.leavesQty());
        }

        private void execute(RingBufferEvent executed) {
            reduceOrder(executed.getOrderId(), executed.getFillQty());
        }

        private void execute(OrderExecuted executed) {
            reduceOrder(executed.orderId(), executed.fillQty());
        }

        private void cancel(RingBufferEvent cancelled) {
            reduceOrder(cancelled.getOrderId(), cancelled.getCancelledQty());
        }

        private void cancel(OrderCancelled cancelled) {
            reduceOrder(cancelled.orderId(), cancelled.cancelledQty());
        }

        private void restate(RingBufferEvent restated) {
            OrderProjection order = orders.get(restated.getOrderId());
            if (order != null) {
                addLevel(order.side, order.price, -order.quantity);
                if (restated.getLeavesQty() > 0) {
                    order.reset(order.side, restated.getPrice(), restated.getLeavesQty());
                    addLevel(order.side, restated.getPrice(), restated.getLeavesQty());
                } else {
                    removeProjection(restated.getOrderId());
                }
            }
        }

        private void restate(OrderRestated restated) {
            OrderProjection order = orders.get(restated.orderId());
            if (order != null) {
                addLevel(order.side, order.price, -order.quantity);
                if (restated.leavesQty() > 0) {
                    order.reset(order.side, restated.price(), restated.leavesQty());
                    addLevel(order.side, restated.price(), restated.leavesQty());
                } else {
                    removeProjection(restated.orderId());
                }
            }
        }

        private void reduceOrder(String orderId, long quantity) {
            OrderProjection order = orders.get(orderId);
            if (order == null || quantity <= 0) {
                return;
            }
            long reduced = Math.min(order.quantity, quantity);
            addLevel(order.side, order.price, -reduced);
            long remaining = order.quantity - reduced;
            if (remaining <= 0) {
                removeProjection(orderId);
            } else {
                order.quantity = remaining;
            }
        }

        private void putProjection(String orderId, Side side, Price price, long quantity) {
            OrderProjection order = orders.get(orderId);
            if (order != null) {
                addLevel(order.side, order.price, -order.quantity);
            } else {
                order = borrowProjection();
                orders.put(orderId, order);
            }
            order.reset(side, price, quantity);
            addLevel(side, price, quantity);
        }

        private void removeProjection(String orderId) {
            OrderProjection order = orders.remove(orderId);
            if (order != null) {
                recycle(order);
            }
        }

        private void addLevel(Side side, Price price, long quantityDelta) {
            NavigableMap<Price, Long> levels = side == Side.BUY ? bids : asks;
            levels.compute(price, (ignored, current) -> {
                long updated = (current == null ? 0 : current) + quantityDelta;
                return updated <= 0 ? null : updated;
            });
        }

        private L2Snapshot snapshot(int depth, Instant asOf) {
            return new L2Snapshot(symbol, levels(bids.descendingMap(), depth), levels(asks, depth), asOf);
        }

        private List<L2Level> levels(NavigableMap<Price, Long> source, int depth) {
            ArrayList<L2Level> levels = new ArrayList<>(Math.min(depth, source.size()));
            for (Map.Entry<Price, Long> entry : source.entrySet()) {
                if (levels.size() == depth) {
                    break;
                }
                levels.add(new L2Level(entry.getKey(), entry.getValue()));
            }
            return levels;
        }
    }

    private static final class OrderProjection {
        private Side side;
        private Price price;
        private long quantity;

        private void reset(Side side, Price price, long quantity) {
            this.side = side;
            this.price = price;
            this.quantity = quantity;
        }

        private void clear() {
            side = null;
            price = null;
            quantity = 0L;
        }
    }

    private static final class ProjectionPoolExhaustedException extends RuntimeException {
        private static final ProjectionPoolExhaustedException INSTANCE = new ProjectionPoolExhaustedException();

        private ProjectionPoolExhaustedException() {
            super("OrderProjection pool exhausted; increase MDE_ORDER_PROJECTION_POOL_SIZE", null, false, false);
        }

        @Override
        public synchronized Throwable fillInStackTrace() {
            return this;
        }
    }

    private static final class MutableCandle {
        private final String symbol;
        private final Instant windowStart;
        private final Instant windowEnd;
        private final Price open;
        private Price high;
        private Price low;
        private Price close;
        private long volume;

        private MutableCandle(String symbol, Instant windowStart, Instant windowEnd, Price open) {
            this.symbol = symbol;
            this.windowStart = windowStart;
            this.windowEnd = windowEnd;
            this.open = open;
            this.high = open;
            this.low = open;
            this.close = open;
        }

        private void add(Price price, long quantity) {
            if (Comparator.<Price>naturalOrder().compare(price, high) > 0) {
                high = price;
            }
            if (Comparator.<Price>naturalOrder().compare(price, low) < 0) {
                low = price;
            }
            close = price;
            volume += quantity;
        }

        private OhlcvCandle toClosed() {
            return new OhlcvCandle(symbol, windowStart, windowEnd, open, high, low, close, volume);
        }
    }
}
