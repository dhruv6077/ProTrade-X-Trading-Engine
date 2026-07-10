package exchange.core;

import Price.Price;
import exchange.dispatch.EventBatchSink;
import exchange.dispatch.MutableEventBatch;
import exchange.model.AdminCommand;
import exchange.model.AdminEvent;
import exchange.model.AdminOperation;
import exchange.model.CancelOrderCommand;
import exchange.model.ExchangeEvent;
import exchange.model.ModifyOrderCommand;
import exchange.model.MutableAdminCommand;
import exchange.model.MutableCancelOrderCommand;
import exchange.model.MutableModifyOrderCommand;
import exchange.model.MutableOrderCommand;
import exchange.model.NewOrderCommand;
import exchange.model.OrderAccepted;
import exchange.model.OrderCancelled;
import exchange.model.OrderCommand;
import exchange.model.OrderExecuted;
import exchange.model.OrderRejected;
import exchange.model.OrderRestated;
import exchange.model.OrderState;
import exchange.model.OrderType;
import exchange.model.RejectReason;
import exchange.model.SelfTradePreventionMode;
import exchange.model.Side;

import java.util.ArrayDeque;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

public final class DeterministicMatchingEngine implements MatchingEngine {
    private static final int ORDER_BOOK_INITIAL_CAPACITY = configuredInitialCapacity();
    private final Map<String, SymbolOrderBook> booksBySymbol = new ConcurrentHashMap<>();
    private volatile SymbolOrderBook[] booksBySymbolId = new SymbolOrderBook[16];
    private int nextFallbackSymbolId = 1;

    public DeterministicMatchingEngine(Iterable<String> symbols) {
        for (String symbol : symbols) {
            addSymbol(symbol);
        }
    }

    public synchronized void addSymbol(String symbol) {
        if (booksBySymbol.containsKey(symbol)) {
            return;
        }
        addSymbol(symbol, nextFallbackSymbolId++);
    }

    public synchronized void addSymbol(String symbol, int symbolId) {
        SymbolOrderBook book = booksBySymbol.computeIfAbsent(symbol, SymbolOrderBook::new);
        if (symbolId <= 0) {
            return;
        }
        SymbolOrderBook[] books = booksBySymbolId;
        if (symbolId >= books.length) {
            int nextLength = books.length;
            while (symbolId >= nextLength) {
                nextLength <<= 1;
            }
            SymbolOrderBook[] expanded = new SymbolOrderBook[nextLength];
            System.arraycopy(books, 0, expanded, 0, books.length);
            booksBySymbolId = expanded;
            books = expanded;
        }
        if (books[symbolId] == null) {
            books[symbolId] = book;
        }
    }

    @Override
    public List<ExchangeEvent> process(OrderCommand command) {
        SymbolOrderBook book = bookFor(command);
        if (book == null) {
            return SingleEventList.of(reject(command, RejectReason.INVALID_SYMBOL, "Unknown symbol"));
        }
        return switch (command) {
            case AdminCommand adminCommand -> book.admin(adminCommand);
            case MutableAdminCommand adminCommand -> book.admin(adminCommand);
            case NewOrderCommand newOrder -> book.newOrder(newOrder);
            case MutableOrderCommand newOrder -> book.newOrder(newOrder);
            case CancelOrderCommand cancelOrder -> book.cancel(cancelOrder);
            case MutableCancelOrderCommand cancelOrder -> book.cancel(cancelOrder);
            case ModifyOrderCommand modifyOrder -> book.modify(modifyOrder);
            case MutableModifyOrderCommand modifyOrder -> book.modify(modifyOrder);
            default -> SingleEventList.of(reject(command, RejectReason.INVALID_ORDER_TYPE, "Unsupported command type"));
        };
    }

    @Override
    public MutableEventBatch processInto(OrderCommand command, MutableEventBatch batch) {
        batch.reset();
        SymbolOrderBook book = bookFor(command);
        if (book == null) {
            batch.add(reject(command, RejectReason.INVALID_SYMBOL, "Unknown symbol"));
            batch.updateBatchSize();
            return batch;
        }
        switch (command) {
            case NewOrderCommand newOrder -> book.newOrder(newOrder, batch);
            case MutableOrderCommand newOrder -> book.newOrder(newOrder, batch);
            default -> batch.addAll(process(command));
        }
        batch.updateBatchSize();
        return batch;
    }

    public void hydrate(List<ExchangeEvent> events) {
        for (ExchangeEvent event : events) {
            addSymbol(event.symbol());
            SymbolOrderBook book = booksBySymbol.get(event.symbol());
            book.hydrate(event);
        }
    }

    private SymbolOrderBook bookFor(OrderCommand command) {
        int symbolId = command.symbolId();
        if (symbolId > 0) {
            SymbolOrderBook[] books = booksBySymbolId;
            if (symbolId < books.length) {
                SymbolOrderBook book = books[symbolId];
                if (book != null) {
                    return book;
                }
            }
        }
        return booksBySymbol.get(command.symbol());
    }

    private static OrderRejected reject(OrderCommand command, RejectReason reason, String message) {
        return new OrderRejected(command.sequenceNumber(), command.orderId(), command.clientId(), command.symbol(),
                reason, message, command.inboundTimestamp());
    }

    private static int configuredInitialCapacity() {
        String configured = System.getProperty("orderBookInitialCapacity");
        if (configured == null || configured.isBlank()) {
            configured = System.getenv("ORDER_BOOK_INITIAL_CAPACITY");
        }
        int expected = configured == null || configured.isBlank() ? 1_024 : Integer.parseInt(configured);
        return Math.max(16, (int) Math.min(1 << 30, (long) (expected / 0.75d) + 1L));
    }

    private static final class SymbolOrderBook {
        private final String symbol;
        private final NavigableMap<Price, ArrayDeque<RestingOrder>> bids = new TreeMap<>();
        private final NavigableMap<Price, ArrayDeque<RestingOrder>> asks = new TreeMap<>();
        private final Map<String, RestingOrder> byOrderId = new HashMap<>(ORDER_BOOK_INITIAL_CAPACITY);
        private final EventBatch eventBatch = new EventBatch(512);
        private boolean halted;

        private SymbolOrderBook(String symbol) {
            this.symbol = symbol;
        }

        private List<ExchangeEvent> newOrder(NewOrderCommand command) {
            EventBatch events = eventBatch.reset();
            newOrder(command.sequenceNumber(), command.inboundTimestamp(), command.orderId(), command.clientId(),
                    command.side(), command.orderType(), command.price(), command.quantity(), command.stpMode(),
                    events);
            return events;
        }

        private List<ExchangeEvent> newOrder(MutableOrderCommand command) {
            EventBatch events = eventBatch.reset();
            newOrder(command.sequenceNumber(), command.inboundTimestamp(), command.orderId(), command.clientId(),
                    command.side(), command.orderType(), command.price(), command.quantity(), command.stpMode(),
                    events);
            return events;
        }

        private MutableEventBatch newOrder(NewOrderCommand command, MutableEventBatch events) {
            newOrder(command.sequenceNumber(), command.inboundTimestamp(), command.orderId(), command.clientId(),
                    command.side(), command.orderType(), command.price(), command.quantity(), command.stpMode(),
                    events);
            return events;
        }

        private MutableEventBatch newOrder(MutableOrderCommand command, MutableEventBatch events) {
            newOrder(command.sequenceNumber(), command.inboundTimestamp(), command.orderId(), command.clientId(),
                    command.side(), command.orderType(), command.price(), command.quantity(), command.stpMode(),
                    events);
            return events;
        }

        private List<ExchangeEvent> newOrder(long sequenceNumber, java.time.Instant inboundTimestamp, String orderId,
                String clientId, Side side, OrderType orderType, Price price, int quantity,
                SelfTradePreventionMode stpMode) {
            EventBatch events = eventBatch.reset();
            newOrder(sequenceNumber, inboundTimestamp, orderId, clientId, side, orderType, price, quantity, stpMode,
                    events);
            return events;
        }

        private void newOrder(long sequenceNumber, java.time.Instant inboundTimestamp, String orderId,
                String clientId, Side side, OrderType orderType, Price price, int quantity,
                SelfTradePreventionMode stpMode, EventBatchSink events) {

            if (halted) {
                events.addRejected(sequenceNumber, orderId, clientId, symbol,
                        RejectReason.MARKET_HALTED, "Market is halted", inboundTimestamp);
                return;
            }
            if (byOrderId.containsKey(orderId)) {
                events.addRejected(sequenceNumber, orderId, clientId, symbol,
                        RejectReason.INVALID_ORDER_ID, "Duplicate order_id", inboundTimestamp);
                return;
            }
            if (orderType == OrderType.STOP || orderType == OrderType.STOP_LIMIT) {
                events.addRejected(sequenceNumber, orderId, clientId, symbol,
                        RejectReason.UNSUPPORTED_ORDER_TYPE, "Stop order trigger book is not enabled",
                        inboundTimestamp);
                return;
            }
            if (orderType == OrderType.FOK && fillableQuantity(clientId, side, price, quantity) < quantity) {
                events.addCancelled(sequenceNumber, orderId, clientId, symbol,
                        quantity, "FOK order could not fully fill", inboundTimestamp);
                return;
            }

            int aggressiveLeavesQty = quantity;
            int aggressiveCumQty = 0;
            events.addAccepted(sequenceNumber, orderId, clientId, symbol, side, orderType, price, quantity,
                    aggressiveLeavesQty, aggressiveCumQty, inboundTimestamp);

            while (aggressiveLeavesQty > 0 && canMatch(side, orderType, price)) {
                ArrayDeque<RestingOrder> restingQueue = topQueue(side.opposite());
                RestingOrder passive = restingQueue.peekFirst();
                if (passive == null) {
                    removeEmptyTop(side.opposite());
                    continue;
                }

                if (passive.clientId.equals(clientId)) {
                    if (stpMode == SelfTradePreventionMode.CANCEL_OLDEST) {
                        restingQueue.removeFirst();
                        byOrderId.remove(passive.orderId);
                        events.addCancelled(sequenceNumber, passive.orderId,
                                passive.clientId, symbol, passive.leavesQty,
                                "Self-trade prevention cancelled oldest resting order", inboundTimestamp);
                        removePriceLevelIfEmpty(side.opposite(), passive.price);
                        continue;
                    }
                    if (stpMode == SelfTradePreventionMode.DECREMENT_LARGER) {
                        if (aggressiveLeavesQty >= passive.leavesQty) {
                            int decrementedQty = passive.leavesQty;
                            aggressiveLeavesQty -= decrementedQty;
                            restingQueue.removeFirst();
                            byOrderId.remove(passive.orderId);
                            events.addCancelled(sequenceNumber, passive.orderId,
                                    passive.clientId, symbol, decrementedQty,
                                    "Self-trade prevention decremented larger order", inboundTimestamp);
                            removePriceLevelIfEmpty(side.opposite(), passive.price);
                            continue;
                        }

                        passive.leavesQty -= aggressiveLeavesQty;
                        events.addRestated(sequenceNumber, passive.orderId,
                                passive.clientId, symbol, passive.price, passive.quantity,
                                passive.leavesQty, passive.cumQty, inboundTimestamp);
                        events.addCancelled(sequenceNumber, orderId, clientId,
                                symbol, aggressiveLeavesQty,
                                "Self-trade prevention decremented larger resting order", inboundTimestamp);
                        return;
                    }
                    events.addCancelled(sequenceNumber, orderId, clientId, symbol,
                            aggressiveLeavesQty, "Self-trade prevention cancelled newest order",
                            inboundTimestamp);
                    return;
                }

                int fillQty = Math.min(aggressiveLeavesQty, passive.leavesQty);
                Price fillPrice = passive.price;
                aggressiveLeavesQty -= fillQty;
                aggressiveCumQty += fillQty;
                passive.leavesQty -= fillQty;
                passive.cumQty += fillQty;

                events.addExecuted(sequenceNumber, orderId, clientId,
                        symbol, passive.orderId, passive.clientId, side, fillPrice,
                        fillQty, aggressiveLeavesQty, aggressiveCumQty, aggressiveLeavesQty == 0,
                        0, 0, inboundTimestamp);
                events.addExecuted(sequenceNumber, passive.orderId, passive.clientId,
                        symbol, orderId, clientId, passive.side, fillPrice,
                        fillQty, passive.leavesQty, passive.cumQty, passive.leavesQty == 0,
                        0, 0, inboundTimestamp);

                if (passive.leavesQty == 0) {
                    restingQueue.removeFirst();
                    byOrderId.remove(passive.orderId);
                    removePriceLevelIfEmpty(passive.side, passive.price);
                }
            }

            if (aggressiveLeavesQty > 0) {
                if (orderType == OrderType.LIMIT) {
                    RestingOrder restingOrder = new RestingOrder(orderId, clientId, symbol, side,
                            orderType, price, quantity, aggressiveLeavesQty, aggressiveCumQty, sequenceNumber);
                    queueFor(side, price).addLast(restingOrder);
                    byOrderId.put(orderId, restingOrder);
                } else {
                    events.addCancelled(sequenceNumber, orderId, clientId,
                            symbol, aggressiveLeavesQty, orderType + " residual cancelled",
                            inboundTimestamp);
                }
            }

        }

        private List<ExchangeEvent> admin(AdminCommand command) {
            return admin(command.sequenceNumber(), command.inboundTimestamp(), command.orderId(), command.clientId(),
                    command.operation());
        }

        private List<ExchangeEvent> admin(MutableAdminCommand command) {
            return admin(command.sequenceNumber(), command.inboundTimestamp(), command.orderId(), command.clientId(),
                    command.operation());
        }

        private List<ExchangeEvent> admin(long sequenceNumber, java.time.Instant inboundTimestamp, String orderId,
                String clientId, AdminOperation operation) {
            AdminCommand command = new AdminCommand(sequenceNumber, inboundTimestamp, orderId, clientId, symbol,
                    operation);
            return switch (command.operation()) {
                case HALT_SYMBOL -> halt(command);
                case RESUME_SYMBOL -> resume(command);
                case CANCEL_ALL_FOR_CLIENT -> cancelAllForClient(command);
                case FLUSH_SNAPSHOT -> flushSnapshot(command);
            };
        }

        private List<ExchangeEvent> halt(AdminCommand command) {
            halted = true;
            return eventBatch.reset().addEvent(new AdminEvent(command.sequenceNumber(), command.orderId(),
                    command.clientId(), symbol, AdminOperation.HALT_SYMBOL, "Symbol halted",
                    command.inboundTimestamp()));
        }

        private List<ExchangeEvent> resume(AdminCommand command) {
            halted = false;
            return eventBatch.reset().addEvent(new AdminEvent(command.sequenceNumber(), command.orderId(),
                    command.clientId(), symbol, AdminOperation.RESUME_SYMBOL, "Symbol resumed",
                    command.inboundTimestamp()));
        }

        private List<ExchangeEvent> flushSnapshot(AdminCommand command) {
            return eventBatch.reset().addEvent(new AdminEvent(command.sequenceNumber(), command.orderId(),
                    command.clientId(), symbol, AdminOperation.FLUSH_SNAPSHOT, "Snapshot flush requested",
                    command.inboundTimestamp()));
        }

        private List<ExchangeEvent> cancelAllForClient(AdminCommand command) {
            EventBatch events = eventBatch.reset();
            events.add(new AdminEvent(command.sequenceNumber(), command.orderId(), command.clientId(), symbol,
                    AdminOperation.CANCEL_ALL_FOR_CLIENT, "Mass cancel requested", command.inboundTimestamp()));
            ArrayList<String> cancelOrderIds = new ArrayList<>();
            for (Map.Entry<String, RestingOrder> entry : byOrderId.entrySet()) {
                if (entry.getValue().clientId.equals(command.clientId())) {
                    cancelOrderIds.add(entry.getKey());
                }
            }
            for (String orderId : cancelOrderIds) {
                RestingOrder restingOrder = byOrderId.remove(orderId);
                if (restingOrder == null) {
                    continue;
                }
                ArrayDeque<RestingOrder> queue = queueFor(restingOrder.side, restingOrder.price);
                queue.remove(restingOrder);
                removePriceLevelIfEmpty(restingOrder.side, restingOrder.price);
                events.add(new OrderCancelled(command.sequenceNumber(), restingOrder.orderId,
                        restingOrder.clientId, symbol, restingOrder.leavesQty,
                        "Admin mass cancel", command.inboundTimestamp()));
            }
            return events;
        }

        private List<ExchangeEvent> cancel(CancelOrderCommand command) {
            return cancel(command.sequenceNumber(), command.inboundTimestamp(), command.orderId(), command.clientId());
        }

        private List<ExchangeEvent> cancel(MutableCancelOrderCommand command) {
            return cancel(command.sequenceNumber(), command.inboundTimestamp(), command.orderId(), command.clientId());
        }

        private List<ExchangeEvent> cancel(long sequenceNumber, java.time.Instant inboundTimestamp, String orderId,
                String clientId) {
            RestingOrder restingOrder = byOrderId.remove(orderId);
            if (restingOrder == null || !restingOrder.clientId.equals(clientId)) {
                OrderCommand command = new CancelOrderCommand(sequenceNumber, inboundTimestamp, orderId, clientId,
                        symbol);
                return eventBatch.reset().addEvent(reject(command, RejectReason.UNKNOWN_ORDER, "Order not found for client"));
            }

            ArrayDeque<RestingOrder> queue = queueFor(restingOrder.side, restingOrder.price);
            queue.remove(restingOrder);
            removePriceLevelIfEmpty(restingOrder.side, restingOrder.price);
            return eventBatch.reset().addEvent(new OrderCancelled(sequenceNumber, orderId, clientId, symbol,
                    restingOrder.leavesQty, "Client cancel", inboundTimestamp));
        }

        private List<ExchangeEvent> modify(ModifyOrderCommand command) {
            return modify(command.sequenceNumber(), command.inboundTimestamp(), command.orderId(), command.clientId(),
                    command.newPrice(), command.newQuantity());
        }

        private List<ExchangeEvent> modify(MutableModifyOrderCommand command) {
            return modify(command.sequenceNumber(), command.inboundTimestamp(), command.orderId(), command.clientId(),
                    command.newPrice(), command.newQuantity());
        }

        private List<ExchangeEvent> modify(long sequenceNumber, java.time.Instant inboundTimestamp, String orderId,
                String clientId, Price newPrice, int newQuantity) {
            RestingOrder restingOrder = byOrderId.get(orderId);
            if (restingOrder == null || !restingOrder.clientId.equals(clientId)) {
                OrderCommand command = new ModifyOrderCommand(sequenceNumber, inboundTimestamp, orderId, clientId,
                        symbol, newPrice, newQuantity);
                return eventBatch.reset().addEvent(reject(command, RejectReason.UNKNOWN_ORDER, "Order not found for client"));
            }
            Side previousSide = restingOrder.side;
            Price previousPrice = restingOrder.price;
            boolean keepsPriority = previousPrice.equals(newPrice)
                    && newQuantity <= restingOrder.quantity;
            if (!keepsPriority) {
                ArrayDeque<RestingOrder> oldQueue = queueFor(previousSide, previousPrice);
                oldQueue.remove(restingOrder);
                removePriceLevelIfEmpty(previousSide, previousPrice);
            }
            restingOrder.restate(newPrice, newQuantity, sequenceNumber);
            if (restingOrder.leavesQty <= 0) {
                ArrayDeque<RestingOrder> queue = queueFor(previousSide, previousPrice);
                queue.remove(restingOrder);
                byOrderId.remove(restingOrder.orderId);
                removePriceLevelIfEmpty(previousSide, previousPrice);
            } else if (!keepsPriority) {
                queueFor(restingOrder.side, restingOrder.price).addLast(restingOrder);
            }
            return eventBatch.reset().addEvent(new OrderRestated(sequenceNumber, orderId, clientId, symbol,
                    newPrice, newQuantity, restingOrder.leavesQty,
                    restingOrder.cumQty, inboundTimestamp));
        }

        private void hydrate(ExchangeEvent event) {
            if (event instanceof OrderAccepted accepted) {
                hydrateAccepted(accepted);
            } else if (event instanceof OrderExecuted executed) {
                hydrateExecution(executed);
            } else if (event instanceof OrderCancelled cancelled) {
                hydrateCancel(cancelled);
            } else if (event instanceof OrderRestated restated) {
                hydrateRestate(restated);
            } else if (event instanceof AdminEvent adminEvent) {
                hydrateAdmin(adminEvent);
            }
        }

        private void hydrateAdmin(AdminEvent event) {
            if (event.operation() == AdminOperation.HALT_SYMBOL) {
                halted = true;
            } else if (event.operation() == AdminOperation.RESUME_SYMBOL) {
                halted = false;
            }
        }

        private void hydrateAccepted(OrderAccepted accepted) {
            OrderState order = accepted.order();
            if (order.orderType() != OrderType.LIMIT || order.price() == null || order.leavesQty() <= 0
                    || byOrderId.containsKey(order.orderId())) {
                return;
            }
            RestingOrder restingOrder = new RestingOrder(order);
            queueFor(order.side(), order.price()).addLast(restingOrder);
            byOrderId.put(order.orderId(), restingOrder);
        }

        private void hydrateExecution(OrderExecuted executed) {
            RestingOrder order = byOrderId.get(executed.orderId());
            if (order == null || executed.fillQty() <= 0) {
                return;
            }
            order.execute(Math.min(order.leavesQty, executed.fillQty()));
            if (order.leavesQty <= 0) {
                ArrayDeque<RestingOrder> queue = queueFor(order.side, order.price);
                queue.remove(order);
                byOrderId.remove(order.orderId);
                removePriceLevelIfEmpty(order.side, order.price);
            }
        }

        private void hydrateCancel(OrderCancelled cancelled) {
            RestingOrder order = byOrderId.get(cancelled.orderId());
            if (order == null) {
                return;
            }
            int remaining = Math.max(0, order.leavesQty - cancelled.cancelledQty());
            if (remaining == 0) {
                ArrayDeque<RestingOrder> queue = queueFor(order.side, order.price);
                queue.remove(order);
                byOrderId.remove(order.orderId);
                removePriceLevelIfEmpty(order.side, order.price);
            } else {
                order.leavesQty = remaining;
            }
        }

        private void hydrateRestate(OrderRestated restated) {
            RestingOrder order = byOrderId.get(restated.orderId());
            if (order == null) {
                return;
            }
            Side previousSide = order.side;
            Price previousPrice = order.price;
            ArrayDeque<RestingOrder> oldQueue = queueFor(previousSide, previousPrice);
            oldQueue.remove(order);
            removePriceLevelIfEmpty(previousSide, previousPrice);
            order.price = restated.price();
            order.quantity = restated.quantity();
            order.leavesQty = restated.leavesQty();
            order.cumQty = restated.cumQty();
            order.sequenceNumber = restated.sequenceNumber();
            if (order.leavesQty > 0) {
                queueFor(order.side, order.price).addLast(order);
            } else {
                byOrderId.remove(order.orderId);
            }
        }

        private int fillableQuantity(NewOrderCommand command) {
            return fillableQuantity(command.clientId(), command.side(), command.price(), command.quantity());
        }

        private int fillableQuantity(String clientId, Side side, Price price, int quantity) {
            int fillable = 0;
            NavigableMap<Price, ArrayDeque<RestingOrder>> contra = side == Side.BUY ? asks : bids;
            Iterable<Map.Entry<Price, ArrayDeque<RestingOrder>>> levels = side == Side.BUY
                    ? contra.entrySet()
                    : contra.descendingMap().entrySet();

            for (Map.Entry<Price, ArrayDeque<RestingOrder>> level : levels) {
                if (!priceCrosses(side, price, level.getKey())) {
                    break;
                }
                for (RestingOrder restingOrder : level.getValue()) {
                    if (!restingOrder.clientId.equals(clientId)) {
                        fillable += restingOrder.leavesQty;
                        if (fillable >= quantity) {
                            return fillable;
                        }
                    }
                }
            }
            return fillable;
        }

        private boolean canMatch(Side side, OrderType orderType, Price limitPrice) {
            Price bestContra = topPrice(side.opposite());
            if (bestContra == null) {
                return false;
            }
            return orderType == OrderType.MARKET || priceCrosses(side, limitPrice, bestContra);
        }

        private boolean priceCrosses(Side side, Price aggressivePrice, Price passivePrice) {
            if (aggressivePrice == null) {
                return true;
            }
            return side == Side.BUY
                    ? aggressivePrice.compareTo(passivePrice) >= 0
                    : aggressivePrice.compareTo(passivePrice) <= 0;
        }

        private Price topPrice(Side side) {
            NavigableMap<Price, ArrayDeque<RestingOrder>> sideMap = side == Side.BUY ? bids : asks;
            if (sideMap.isEmpty()) {
                return null;
            }
            return side == Side.BUY ? sideMap.lastKey() : sideMap.firstKey();
        }

        private ArrayDeque<RestingOrder> topQueue(Side side) {
            Price topPrice = topPrice(side);
            return topPrice == null ? null : queueFor(side, topPrice);
        }

        private ArrayDeque<RestingOrder> queueFor(Side side, Price price) {
            NavigableMap<Price, ArrayDeque<RestingOrder>> sideMap = side == Side.BUY ? bids : asks;
            return sideMap.computeIfAbsent(price, ignored -> new ArrayDeque<>());
        }

        private void removeEmptyTop(Side side) {
            Price topPrice = topPrice(side);
            if (topPrice != null) {
                removePriceLevelIfEmpty(side, topPrice);
            }
        }

        private void removePriceLevelIfEmpty(Side side, Price price) {
            NavigableMap<Price, ArrayDeque<RestingOrder>> sideMap = side == Side.BUY ? bids : asks;
            ArrayDeque<RestingOrder> queue = sideMap.get(price);
            if (queue != null && queue.isEmpty()) {
                sideMap.remove(price);
            }
        }
    }

    private static final class RestingOrder {
        private final String orderId;
        private final String clientId;
        private final String symbol;
        private final Side side;
        private final OrderType orderType;
        private Price price;
        private int quantity;
        private int leavesQty;
        private int cumQty;
        private long sequenceNumber;

        private RestingOrder(OrderState state) {
            this(state.orderId(), state.clientId(), state.symbol(), state.side(), state.orderType(), state.price(),
                    state.quantity(), state.leavesQty(), state.cumQty(), state.sequenceNumber());
        }

        private RestingOrder(String orderId, String clientId, String symbol, Side side, OrderType orderType,
                Price price, int quantity, int leavesQty, int cumQty, long sequenceNumber) {
            this.orderId = orderId;
            this.clientId = clientId;
            this.symbol = symbol;
            this.side = side;
            this.orderType = orderType;
            this.price = price;
            this.quantity = quantity;
            this.leavesQty = leavesQty;
            this.cumQty = cumQty;
            this.sequenceNumber = sequenceNumber;
        }

        private void execute(int fillQty) {
            leavesQty -= fillQty;
            cumQty += fillQty;
        }

        private void restate(Price newPrice, int newQuantity, long newSequenceNumber) {
            price = newPrice;
            quantity = newQuantity;
            leavesQty = Math.max(0, newQuantity - cumQty);
            sequenceNumber = newSequenceNumber;
        }
    }

    private static final class EventBatch extends AbstractList<ExchangeEvent> implements EventBatchSink {
        private final ExchangeEvent[] events;
        private int size;

        private EventBatch(int capacity) {
            this.events = new ExchangeEvent[capacity];
        }

        private EventBatch reset() {
            for (int i = 0; i < size; i++) {
                events[i] = null;
            }
            size = 0;
            return this;
        }

        private EventBatch addEvent(ExchangeEvent event) {
            add(event);
            return this;
        }

        @Override
        public boolean add(ExchangeEvent event) {
            if (size == events.length) {
                throw new IllegalStateException("Matching event batch capacity exceeded");
            }
            events[size++] = event;
            return true;
        }

        @Override
        public void addAccepted(long sequenceNumber, String orderId, String clientId, String symbol,
                Side side, OrderType orderType, Price price, int quantity, int leavesQty, int cumQty,
                java.time.Instant eventTimestamp) {
            add(new OrderAccepted(sequenceNumber, orderId, clientId, symbol,
                    new OrderState(orderId, clientId, symbol, side, orderType, price, quantity, leavesQty, cumQty,
                            sequenceNumber),
                    eventTimestamp));
        }

        @Override
        public void addRejected(long sequenceNumber, String orderId, String clientId, String symbol,
                RejectReason reason, String message, java.time.Instant eventTimestamp) {
            add(new OrderRejected(sequenceNumber, orderId, clientId, symbol, reason, message, eventTimestamp));
        }

        @Override
        public void addExecuted(long sequenceNumber, String orderId, String clientId, String symbol,
                String contraOrderId, String contraClientId, Side side, Price fillPrice, int fillQty,
                int leavesQty, int cumQty, boolean fullFill, long engineInNanos, long eventEmittedNanos,
                java.time.Instant eventTimestamp) {
            add(new OrderExecuted(sequenceNumber, orderId, clientId, symbol, contraOrderId, contraClientId,
                    side, fillPrice, fillQty, leavesQty, cumQty, fullFill, engineInNanos, eventEmittedNanos,
                    eventTimestamp));
        }

        @Override
        public void addCancelled(long sequenceNumber, String orderId, String clientId, String symbol,
                int cancelledQty, String reason, java.time.Instant eventTimestamp) {
            add(new OrderCancelled(sequenceNumber, orderId, clientId, symbol, cancelledQty, reason, eventTimestamp));
        }

        @Override
        public void addRestated(long sequenceNumber, String orderId, String clientId, String symbol,
                Price price, int quantity, int leavesQty, int cumQty, java.time.Instant eventTimestamp) {
            add(new OrderRestated(sequenceNumber, orderId, clientId, symbol, price, quantity, leavesQty, cumQty,
                    eventTimestamp));
        }

        @Override
        public void addAdmin(long sequenceNumber, String orderId, String clientId, String symbol,
                AdminOperation operation, String message, java.time.Instant eventTimestamp) {
            add(new AdminEvent(sequenceNumber, orderId, clientId, symbol, operation, message, eventTimestamp));
        }

        @Override
        public ExchangeEvent get(int index) {
            if (index < 0 || index >= size) {
                throw new IndexOutOfBoundsException(index);
            }
            return events[index];
        }

        @Override
        public int size() {
            return size;
        }
    }

    private static final class SingleEventList extends AbstractList<ExchangeEvent> {
        private static final ThreadLocal<SingleEventList> LOCAL = ThreadLocal.withInitial(SingleEventList::new);
        private ExchangeEvent event;

        private static SingleEventList of(ExchangeEvent event) {
            SingleEventList list = LOCAL.get();
            list.event = event;
            return list;
        }

        @Override
        public ExchangeEvent get(int index) {
            if (index != 0 || event == null) {
                throw new IndexOutOfBoundsException(index);
            }
            return event;
        }

        @Override
        public int size() {
            return event == null ? 0 : 1;
        }
    }
}
