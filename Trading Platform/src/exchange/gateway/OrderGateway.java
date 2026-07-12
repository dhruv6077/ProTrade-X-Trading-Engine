package exchange.gateway;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.TimeoutException;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import exchange.core.AffinityThreadFactory;
import exchange.core.DisruptorWaitStrategies;
import exchange.core.FailSafeDisruptorExceptionHandler;
import exchange.core.MatchingEngine;
import exchange.core.Sequencer;
import exchange.dispatch.EventDispatcher;
import exchange.dispatch.MutableEventBatch;
import exchange.journal.CommandJournal;
import exchange.model.AdminCommand;
import exchange.model.AdminOperation;
import exchange.model.CancelOrderCommand;
import exchange.model.CommandType;
import exchange.model.ExchangeEvent;
import exchange.model.ModifyOrderCommand;
import exchange.model.MutableAdminCommand;
import exchange.model.MutableCancelOrderCommand;
import exchange.model.MutableModifyOrderCommand;
import exchange.model.MutableOrderCommand;
import exchange.model.NewOrderCommand;
import exchange.model.OrderCommand;
import exchange.model.OrderExecuted;
import exchange.model.OrderRejected;
import exchange.model.RejectReason;
import exchange.model.OrderType;
import exchange.model.SelfTradePreventionMode;
import exchange.model.Side;
import exchange.risk.RiskDecision;
import exchange.risk.RiskEngine;
import exchange.replication.CommandReplicator;
import exchange.replication.NoOpCommandReplicator;
import exchange.telemetry.LatencyTelemetry;
import Price.Price;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.LockSupport;

public final class OrderGateway implements AutoCloseable {
    private static final List<ExchangeEvent> EMPTY_EVENTS = Collections.emptyList();
    private final GatewayValidator validator;
    private final RiskEngine riskEngine;
    private final Sequencer sequencer;
    private final CommandJournal journal;
    private final CommandReplicator replicator;
    private final MatchingEngine matchingEngine;
    private final EventDispatcher dispatcher;
    private volatile ShardProcessor[] shardsBySymbolId = new ShardProcessor[16];
    private final Disruptor<StageOneCommandEvent> riskDisruptor;
    private final RingBuffer<StageOneCommandEvent> riskRingBuffer;
    private final AtomicBoolean acceptingCommands;
    private static final int SHARD_RING_CAPACITY = ringCapacity("shardRingCapacity", "SHARD_RING_CAPACITY", 4_096);
    private static final int RISK_QUEUE_CAPACITY = ringCapacity("riskRingCapacity", "RISK_RING_CAPACITY", 8_192);
    private final NewOrderCommandPool newOrderCommandPool = new NewOrderCommandPool(RISK_QUEUE_CAPACITY);

    public OrderGateway(
            GatewayValidator validator,
            RiskEngine riskEngine,
            Sequencer sequencer,
            CommandJournal journal,
            MatchingEngine matchingEngine,
            EventDispatcher dispatcher) {
        this(validator, riskEngine, sequencer, journal, matchingEngine, dispatcher, true);
    }

    public OrderGateway(
            GatewayValidator validator,
            RiskEngine riskEngine,
            Sequencer sequencer,
            CommandJournal journal,
            CommandReplicator replicator,
            MatchingEngine matchingEngine,
            EventDispatcher dispatcher) {
        this(validator, riskEngine, sequencer, journal, replicator, matchingEngine, dispatcher, true);
    }

    public OrderGateway(
            GatewayValidator validator,
            RiskEngine riskEngine,
            Sequencer sequencer,
            CommandJournal journal,
            MatchingEngine matchingEngine,
            EventDispatcher dispatcher,
            boolean acceptingCommands) {
        this(validator, riskEngine, sequencer, journal, NoOpCommandReplicator.INSTANCE, matchingEngine, dispatcher,
                acceptingCommands);
    }

    public OrderGateway(
            GatewayValidator validator,
            RiskEngine riskEngine,
            Sequencer sequencer,
            CommandJournal journal,
            CommandReplicator replicator,
            MatchingEngine matchingEngine,
            EventDispatcher dispatcher,
            boolean acceptingCommands) {
        this.validator = validator;
        this.riskEngine = riskEngine;
        this.sequencer = sequencer;
        this.journal = journal;
        this.replicator = replicator == null ? NoOpCommandReplicator.INSTANCE : replicator;
        this.matchingEngine = matchingEngine;
        this.dispatcher = dispatcher;
        this.acceptingCommands = new AtomicBoolean(acceptingCommands);
        this.riskDisruptor = newRiskDisruptor();
        this.riskRingBuffer = riskDisruptor.start();
    }

    public List<ExchangeEvent> process(OrderCommand rawCommand) {
        return submit(rawCommand);
    }

    public List<ExchangeEvent> submitNewOrder(
            String orderId,
            String clientId,
            String symbol,
            Side side,
            OrderType orderType,
            Price price,
            int quantity,
            SelfTradePreventionMode stpMode) {
        return submitNewOrder(orderId, clientId, symbol, side, orderType, price, quantity, stpMode, System.nanoTime());
    }

    public List<ExchangeEvent> submitNewOrder(
            String orderId,
            String clientId,
            String symbol,
            Side side,
            OrderType orderType,
            Price price,
            int quantity,
            SelfTradePreventionMode stpMode,
            long ingressTimeNs) {
        MutableOrderCommand pooled = newOrderCommandPool.borrow();
        try {
            pooled.populate(orderId, clientId, symbol, side, orderType, price, quantity, stpMode, ingressTimeNs);
            return submit(pooled);
        } finally {
            pooled.reset();
            newOrderCommandPool.release(pooled);
        }
    }

    public List<ExchangeEvent> submitNewOrderAsync(
            String orderId,
            String clientId,
            String symbol,
            Side side,
            OrderType orderType,
            Price price,
            int quantity,
            SelfTradePreventionMode stpMode) {
        return submitNewOrderAsync(orderId, clientId, symbol, side, orderType, price, quantity, stpMode,
                System.nanoTime());
    }

    public List<ExchangeEvent> submitNewOrderAsync(
            String orderId,
            String clientId,
            String symbol,
            Side side,
            OrderType orderType,
            Price price,
            int quantity,
            SelfTradePreventionMode stpMode,
            long ingressTimeNs) {
        MutableOrderCommand pooled = newOrderCommandPool.borrow();
        try {
            pooled.populate(orderId, clientId, symbol, side, orderType, price, quantity, stpMode, ingressTimeNs);
            return submitAsync(pooled);
        } finally {
            pooled.reset();
            newOrderCommandPool.release(pooled);
        }
    }

    public List<ExchangeEvent> submit(OrderCommand rawCommand) {
        List<ExchangeEvent> events = submitOnRiskStage(rawCommand, false);
        return events.isEmpty() ? EMPTY_EVENTS : List.copyOf(events);
    }

    public List<ExchangeEvent> submitAsync(OrderCommand rawCommand) {
        return submitOnRiskStage(rawCommand, true);
    }

    private List<ExchangeEvent> submitOnRiskStage(OrderCommand rawCommand, boolean asyncMatching) {
        long sequence;
        try {
            sequence = riskRingBuffer.tryNext();
        } catch (InsufficientCapacityException e) {
            return List.of(new OrderRejected(rawCommand.sequenceNumber(), rawCommand.orderId(), rawCommand.clientId(),
                    rawCommand.symbol(), RejectReason.RISK_KILL_SWITCH,
                    "Account risk core is overloaded", rawCommand.inboundTimestamp()));
        }
        StageOneCommandEvent event = riskRingBuffer.get(sequence);
        event.copyCommand(rawCommand, asyncMatching, sequence);
        riskRingBuffer.publish(sequence);
        return awaitStageOneResult(event, sequence, rawCommand);
    }

    private List<ExchangeEvent> submitSyncOnRiskStage(OrderCommand rawCommand, StageOneCommandEvent handoff) {
        if (!acceptingCommands.get()) {
            return handoff.singleResult(new OrderRejected(rawCommand.sequenceNumber(), rawCommand.orderId(), rawCommand.clientId(),
                    rawCommand.symbol(), RejectReason.RISK_KILL_SWITCH,
                    "Order gateway is closed until event-sourced state hydration completes",
                    rawCommand.inboundTimestamp()));
        }

        ValidationResult validation = validator.validate(rawCommand);
        if (!validation.accepted()) {
            return publishSequencedReject(rawCommand, validation.rejectReason(), validation.message(), handoff);
        }
        OrderCommand validatedCommand = commandWithSymbolId(rawCommand);

        RiskDecision riskDecision = riskEngine.check(validatedCommand);
        if (!riskDecision.accepted()) {
            return publishSequencedReject(validatedCommand, riskDecision.rejectReason(), riskDecision.message(), handoff);
        }

        OrderCommand sequencedCommand = sequencer.sequence(validatedCommand);
        journal.append(sequencedCommand);
        replicator.replicate(sequencedCommand, true);
        List<ExchangeEvent> events = processOnSymbolShard(sequencedCommand);
        riskEngine.onEvents(events);
        dispatcher.publish(events);
        return events;
    }

    private List<ExchangeEvent> submitAsyncOnRiskStage(OrderCommand rawCommand, StageOneCommandEvent handoff) {
        if (!acceptingCommands.get()) {
            return handoff.singleResult(new OrderRejected(rawCommand.sequenceNumber(), rawCommand.orderId(), rawCommand.clientId(),
                    rawCommand.symbol(), RejectReason.RISK_KILL_SWITCH,
                    "Order gateway is closed until event-sourced state hydration completes",
                    rawCommand.inboundTimestamp()));
        }

        ValidationResult validation = validator.validate(rawCommand);
        if (!validation.accepted()) {
            return publishSequencedReject(rawCommand, validation.rejectReason(), validation.message(), handoff);
        }
        OrderCommand validatedCommand = commandWithSymbolId(rawCommand);

        RiskDecision riskDecision = riskEngine.check(validatedCommand);
        if (!riskDecision.accepted()) {
            return publishSequencedReject(validatedCommand, riskDecision.rejectReason(), riskDecision.message(), handoff);
        }

        OrderCommand sequencedCommand = sequencer.sequence(validatedCommand);
        journal.append(sequencedCommand);
        replicator.replicate(sequencedCommand, true);
        enqueueOnSymbolShard(sequencedCommand);
        return EMPTY_EVENTS;
    }

    private List<ExchangeEvent> publishSequencedReject(OrderCommand rawCommand, RejectReason reason, String message,
            StageOneCommandEvent handoff) {
        OrderCommand sequencedCommand = sequencer.sequence(rawCommand);
        journal.append(sequencedCommand);
        replicator.replicate(sequencedCommand, false);
        return publishReject(sequencedCommand, reason, message, handoff);
    }

    private List<ExchangeEvent> publishReject(OrderCommand command, RejectReason reason, String message,
            StageOneCommandEvent handoff) {
        OrderRejected rejected = new OrderRejected(command.sequenceNumber(), command.orderId(),
                command.clientId(), command.symbol(), reason, message, command.inboundTimestamp());
        List<ExchangeEvent> events = handoff == null ? List.of(rejected) : handoff.singleResult(rejected);
        dispatcher.publish(events);
        return events;
    }

    private List<ExchangeEvent> processOnSymbolShard(OrderCommand command) {
        ShardProcessor processor = processorFor(command);
        try {
            return processor.process(command);
        } catch (RuntimeException e) {
            return List.of(new OrderRejected(command.sequenceNumber(), command.orderId(), command.clientId(),
                    command.symbol(), RejectReason.INVALID_ORDER_ID,
                    e.getMessage() == null ? "Matching engine failed" : e.getMessage(),
                    command.inboundTimestamp()));
        }
    }

    private void enqueueOnSymbolShard(OrderCommand command) {
        try {
            processorFor(command).enqueue(command);
        } catch (RejectedExecutionException e) {
            riskEngine.releaseReservation(command.orderId());
            List<ExchangeEvent> events = List.of(new OrderRejected(command.sequenceNumber(), command.orderId(),
                    command.clientId(), command.symbol(), RejectReason.RISK_KILL_SWITCH,
                    "Symbol shard is overloaded", command.inboundTimestamp()));
            riskEngine.onEvents(events);
            dispatcher.publish(events);
        }
    }

    private void publishShardEvents(List<ExchangeEvent> events) {
        long sequence;
        try {
            sequence = riskRingBuffer.next();
        } catch (RuntimeException e) {
            riskEngine.onEvents(events);
            dispatcher.publish(events);
            return;
        }
        StageOneCommandEvent event = riskRingBuffer.get(sequence);
        event.copyEvents(events, sequence);
        riskRingBuffer.publish(sequence);
    }

    private void publishShardEvents(MutableEventBatch events) {
        long sequence;
        try {
            sequence = riskRingBuffer.next();
        } catch (RuntimeException e) {
            for (int i = 0; i < events.size(); i++) {
                riskEngine.onEvent(events.get(i));
            }
            dispatcher.publish(events);
            return;
        }
        StageOneCommandEvent event = riskRingBuffer.get(sequence);
        event.copyEvents(events, sequence);
        riskRingBuffer.publish(sequence);
        awaitStageOneApply(event, sequence);
    }

    private ShardProcessor processorFor(OrderCommand command) {
        int symbolId = command.symbolId();
        if (symbolId <= 0) {
            throw new RejectedExecutionException("Command is missing primitive symbolId");
        }
        ShardProcessor[] processors = shardsBySymbolId;
        if (symbolId < processors.length) {
            ShardProcessor existing = processors[symbolId];
            if (existing != null) {
                return existing;
            }
        }
        return createProcessorFor(symbolId, command.symbol());
    }

    private synchronized ShardProcessor createProcessorFor(int symbolId, String symbol) {
        ShardProcessor[] processors = shardsBySymbolId;
        if (symbolId >= processors.length) {
            int nextLength = processors.length;
            while (symbolId >= nextLength) {
                nextLength <<= 1;
            }
            ShardProcessor[] expanded = new ShardProcessor[nextLength];
            System.arraycopy(processors, 0, expanded, 0, processors.length);
            shardsBySymbolId = expanded;
            processors = expanded;
        }
        ShardProcessor existing = processors[symbolId];
        if (existing != null) {
            return existing;
        }
        ShardProcessor created = new ShardProcessor(symbolId, symbol);
        processors[symbolId] = created;
        return created;
    }

    private Disruptor<StageOneCommandEvent> newRiskDisruptor() {
        Disruptor<StageOneCommandEvent> disruptor = new Disruptor<>(
                StageOneCommandEvent::new,
                RISK_QUEUE_CAPACITY,
                new AffinityThreadFactory("account-risk-core"),
                ProducerType.MULTI,
                DisruptorWaitStrategies.latencySensitive());
        disruptor.setDefaultExceptionHandler(new FailSafeDisruptorExceptionHandler<>("account-risk-core",
                (event, sequence, error) -> event.complete(EMPTY_EVENTS, error, sequence)));
        disruptor.handleEventsWith(new StageOneRiskHandler());
        return disruptor;
    }

    private OrderCommand commandWithSymbolId(OrderCommand command) {
        int symbolId = validator.symbolId(command.symbol());
        if (symbolId <= 0 || command.symbolId() == symbolId) {
            return command;
        }
        if (command instanceof MutableOrderCommand mutable) {
            mutable.setSymbolId(symbolId);
            return mutable;
        }
        if (command instanceof MutableModifyOrderCommand mutable) {
            mutable.setSymbolId(symbolId);
            return mutable;
        }
        if (command instanceof MutableCancelOrderCommand mutable) {
            mutable.setSymbolId(symbolId);
            return mutable;
        }
        if (command instanceof MutableAdminCommand mutable) {
            mutable.setSymbolId(symbolId);
            return mutable;
        }
        if (command instanceof NewOrderCommand newOrder) {
            return newOrder.withSymbolId(symbolId);
        }
        if (command instanceof ModifyOrderCommand modify) {
            return modify.withSymbolId(symbolId);
        }
        if (command instanceof CancelOrderCommand cancel) {
            return cancel.withSymbolId(symbolId);
        }
        if (command instanceof AdminCommand admin) {
            return admin.withSymbolId(symbolId);
        }
        return command;
    }

    public List<ShardStatus> shardStatuses() {
        ArrayList<ShardStatus> statuses = new ArrayList<>();
        ShardProcessor[] processors = shardsBySymbolId;
        for (int symbolId = 1; symbolId < processors.length; symbolId++) {
            ShardProcessor processor = processors[symbolId];
            if (processor == null) {
                continue;
            }
            String symbol = validator.symbolFor(symbolId);
            statuses.add(new ShardStatus(symbol == null ? Integer.toString(symbolId) : symbol, processor.running(),
                    processor.queuedCommands(), processor.completedCommands()));
        }
        statuses.sort((left, right) -> left.symbol().compareTo(right.symbol()));
        return List.copyOf(statuses);
    }

    public void pauseIngress() {
        acceptingCommands.set(false);
    }

    public void openIngress() {
        acceptingCommands.set(true);
    }

    public boolean isAcceptingCommands() {
        return acceptingCommands.get();
    }

    private static long latencyStartNanos(OrderCommand command) {
        long ingressTimeNs = command.ingressTimeNs();
        return ingressTimeNs > 0L ? ingressTimeNs : System.nanoTime();
    }

    private static long latencyStartNanos(long ingressTimeNs) {
        return ingressTimeNs > 0L ? ingressTimeNs : System.nanoTime();
    }

    private static int ringCapacity(String propertyName, String envName, int defaultValue) {
        String configured = System.getProperty(propertyName);
        if (configured == null || configured.isBlank()) {
            configured = System.getenv(envName);
        }
        int value = configured == null || configured.isBlank() ? defaultValue : Integer.parseInt(configured);
        if (Integer.bitCount(value) != 1) {
            throw new IllegalArgumentException(propertyName + " must be a power of two: " + value);
        }
        return value;
    }

    private List<ExchangeEvent> withLatency(List<ExchangeEvent> events, long engineInNanos, long eventEmittedNanos) {
        ArrayList<ExchangeEvent> enriched = new ArrayList<>(events.size());
        for (ExchangeEvent event : events) {
            if (event instanceof OrderExecuted executed) {
                LatencyTelemetry.getInstance().recordEngineToDispatch(engineInNanos, eventEmittedNanos);
                enriched.add(new OrderExecuted(
                        executed.sequenceNumber(),
                        executed.orderId(),
                        executed.clientId(),
                        executed.symbol(),
                        executed.contraOrderId(),
                        executed.contraClientId(),
                        executed.side(),
                        executed.fillPrice(),
                        executed.fillQty(),
                        executed.leavesQty(),
                        executed.cumQty(),
                        executed.fullFill(),
                        engineInNanos,
                        eventEmittedNanos,
                        executed.eventTimestamp()));
            } else {
                enriched.add(event);
            }
        }
        return List.copyOf(enriched);
    }

    @Override
    public void close() {
        pauseIngress();
        ShardProcessor[] processors = shardsBySymbolId;
        for (ShardProcessor processor : processors) {
            if (processor == null) {
                continue;
            }
            processor.close();
        }
        try {
            riskDisruptor.shutdown(5, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            riskDisruptor.halt();
        }
    }

    public record ShardStatus(String symbol, boolean running, int queuedCommands, long completedCommands) {
    }

    private final class ShardProcessor implements AutoCloseable {
        private final int symbolId;
        private final String symbol;
        private final Disruptor<ShardEvent> disruptor;
        private final RingBuffer<ShardEvent> ringBuffer;
        private final AtomicBoolean running = new AtomicBoolean(true);
        private final AtomicLong completedCommands = new AtomicLong();

        private ShardProcessor(int symbolId, String symbol) {
            this.symbolId = symbolId;
            this.symbol = symbol;
            this.disruptor = new Disruptor<>(
                    ShardEvent::new,
                    SHARD_RING_CAPACITY,
                    new AffinityThreadFactory("matching-" + symbolId + "-" + symbol),
                    ProducerType.SINGLE,
                    DisruptorWaitStrategies.latencySensitive());
            this.disruptor.setDefaultExceptionHandler(new FailSafeDisruptorExceptionHandler<>("matching-shard",
                    (event, sequence, error) -> event.complete(EMPTY_EVENTS, error, sequence)));
            this.disruptor.handleEventsWith(new ShardEventHandler());
            this.ringBuffer = disruptor.start();
        }

        private List<ExchangeEvent> process(OrderCommand command) {
            long sequence;
            try {
                sequence = ringBuffer.tryNext();
            } catch (InsufficientCapacityException e) {
                throw new RejectedExecutionException("Symbol shard ring is overloaded", e);
            }
            ShardEvent event = ringBuffer.get(sequence);
            event.copyCommand(command, false);
            ringBuffer.publish(sequence);
            return awaitShardResult(event, sequence);
        }

        private void enqueue(OrderCommand command) {
            long sequence;
            try {
                sequence = ringBuffer.tryNext();
            } catch (InsufficientCapacityException e) {
                throw new RejectedExecutionException("Symbol shard ring is overloaded", e);
            }
            ShardEvent event = ringBuffer.get(sequence);
            event.copyCommand(command, true);
            ringBuffer.publish(sequence);
        }

        private List<ExchangeEvent> awaitShardResult(ShardEvent event, long sequence) {
            int spins = 0;
            while (!event.isComplete(sequence)) {
                if (spins < 256) {
                    Thread.onSpinWait();
                } else if (spins < 1_024) {
                    Thread.yield();
                } else {
                    LockSupport.parkNanos(1_000L);
                }
                spins++;
            }
            Throwable error = event.error();
            if (error != null) {
                throw new IllegalStateException(error);
            }
            return event.result();
        }

        private boolean running() {
            return running.get();
        }

        private int queuedCommands() {
            long queued = ringBuffer.getBufferSize() - ringBuffer.remainingCapacity();
            return queued > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) queued;
        }

        private long completedCommands() {
            return completedCommands.get();
        }

        @Override
        public void close() {
            if (!running.compareAndSet(true, false)) {
                return;
            }
            try {
                disruptor.shutdown(5, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                disruptor.halt();
            }
        }

        private final class ShardEventHandler implements EventHandler<ShardEvent> {
            @Override
            public void onEvent(ShardEvent event, long sequence, boolean endOfBatch) {
                try {
                    if (event.asyncMatching) {
                        MutableEventBatch events = processShardEventInto(event);
                        publishShardEvents(events);
                        event.complete(EMPTY_EVENTS, null, sequence);
                    } else {
                        List<ExchangeEvent> events = processShardEvent(event);
                        event.complete(events, null, sequence);
                    }
                } catch (Throwable t) {
                    if (event.asyncMatching) {
                        riskEngine.releaseReservation(event.orderId);
                        List<ExchangeEvent> events = event.singleResult(new OrderRejected(event.sequenceNumber,
                                event.orderId, event.clientId, event.symbol, RejectReason.INVALID_ORDER_ID,
                                t.getMessage() == null ? "Matching engine failed" : t.getMessage(),
                                event.inboundTimestamp));
                        publishShardEvents(events);
                        event.complete(EMPTY_EVENTS, null, sequence);
                    } else {
                        event.complete(EMPTY_EVENTS, t, sequence);
                    }
                } finally {
                    completedCommands.incrementAndGet();
                }
            }

            private List<ExchangeEvent> processShardEvent(ShardEvent event) {
                long engineInNanos = latencyStartNanos(event.ingressTimeNs);
                return withLatency(matchingEngine.process(event.command()), engineInNanos, System.nanoTime());
            }

            private MutableEventBatch processShardEventInto(ShardEvent event) {
                long engineInNanos = latencyStartNanos(event.ingressTimeNs);
                MutableEventBatch events = matchingEngine.processInto(event.command(), event.mutableEvents);
                events.applyLatency(engineInNanos, System.nanoTime());
                return events;
            }
        }
    }

    private static final class ShardEvent {
        private final MutableOrderCommand newOrder = new MutableOrderCommand();
        private final MutableCancelOrderCommand cancelOrder = new MutableCancelOrderCommand();
        private final MutableModifyOrderCommand modifyOrder = new MutableModifyOrderCommand();
        private final MutableAdminCommand adminCommand = new MutableAdminCommand();
        private final SingleEventList singleEventResult = new SingleEventList();
        private final MutableEventBatch mutableEvents = new MutableEventBatch(64);
        private volatile long completedSequence = Long.MIN_VALUE;
        private volatile List<ExchangeEvent> result = EMPTY_EVENTS;
        private volatile Throwable error;
        private boolean asyncMatching;
        private CommandType commandType;
        private long sequenceNumber;
        private java.time.Instant inboundTimestamp = java.time.Instant.EPOCH;
        private String orderId;
        private String clientId;
        private String symbol;
        private int symbolId;
        private Side side;
        private OrderType orderType;
        private Price price;
        private int quantity;
        private SelfTradePreventionMode stpMode;
        private AdminOperation adminOperation;
        private Price newPrice;
        private int newQuantity;
        private long ingressTimeNs;

        private void copyCommand(OrderCommand command, boolean asyncMatching) {
            completedSequence = Long.MIN_VALUE;
            result = EMPTY_EVENTS;
            error = null;
            this.asyncMatching = asyncMatching;
            commandType = command.commandType();
            sequenceNumber = command.sequenceNumber();
            inboundTimestamp = command.inboundTimestamp();
            orderId = command.orderId();
            clientId = command.clientId();
            symbol = command.symbol();
            symbolId = command.symbolId();
            ingressTimeNs = command.ingressTimeNs();
            side = null;
            orderType = null;
            price = null;
            quantity = 0;
            stpMode = null;
            adminOperation = null;
            newPrice = null;
            newQuantity = 0;

            if (command instanceof NewOrderCommand newOrderCommand) {
                side = newOrderCommand.side();
                orderType = newOrderCommand.orderType();
                price = newOrderCommand.price();
                quantity = newOrderCommand.quantity();
                stpMode = newOrderCommand.stpMode();
            } else if (command instanceof MutableOrderCommand mutable) {
                side = mutable.side();
                orderType = mutable.orderType();
                price = mutable.price();
                quantity = mutable.quantity();
                stpMode = mutable.stpMode();
            } else if (command instanceof AdminCommand admin) {
                adminOperation = admin.operation();
            } else if (command instanceof MutableAdminCommand admin) {
                adminOperation = admin.operation();
            } else if (command instanceof ModifyOrderCommand modify) {
                newPrice = modify.newPrice();
                newQuantity = modify.newQuantity();
            } else if (command instanceof MutableModifyOrderCommand modify) {
                newPrice = modify.newPrice();
                newQuantity = modify.newQuantity();
            }
        }

        private OrderCommand command() {
            return switch (commandType) {
                case NEW_ORDER -> {
                    newOrder.populate(orderId, clientId, symbol, symbolId, side, orderType, price, quantity, stpMode,
                            ingressTimeNs);
                    yield newOrder.withSequencing(sequenceNumber, inboundTimestamp);
                }
                case CANCEL_ORDER -> {
                    cancelOrder.populate(sequenceNumber, inboundTimestamp, orderId, clientId, symbol, symbolId);
                    yield cancelOrder;
                }
                case MODIFY_ORDER -> {
                    modifyOrder.populate(sequenceNumber, inboundTimestamp, orderId, clientId, symbol, symbolId,
                            newPrice, newQuantity);
                    yield modifyOrder;
                }
                case ADMIN -> {
                    adminCommand.populate(sequenceNumber, inboundTimestamp, orderId, clientId, symbol, symbolId,
                            adminOperation);
                    yield adminCommand;
                }
            };
        }

        private void complete(List<ExchangeEvent> result, Throwable error, long sequence) {
            this.result = result;
            this.error = error;
            completedSequence = sequence;
        }

        private List<ExchangeEvent> singleResult(ExchangeEvent event) {
            singleEventResult.set(event);
            return singleEventResult;
        }

        private boolean isComplete(long sequence) {
            return completedSequence == sequence;
        }

        private List<ExchangeEvent> result() {
            return result;
        }

        private Throwable error() {
            return error;
        }
    }

    private static final class NewOrderCommandPool {
        private final AtomicReferenceArray<MutableOrderCommand> pool;
        private final AtomicInteger borrowCursor = new AtomicInteger();
        private final AtomicInteger releaseCursor = new AtomicInteger();

        private NewOrderCommandPool(int initialCapacity) {
            this.pool = new AtomicReferenceArray<>(initialCapacity);
            for (int i = 0; i < initialCapacity; i++) {
                pool.set(i, new MutableOrderCommand());
            }
        }

        private MutableOrderCommand borrow() {
            int start = borrowCursor.getAndIncrement();
            int length = pool.length();
            for (int probe = 0; probe < length; probe++) {
                int index = positiveMod(start + probe, length);
                MutableOrderCommand command = pool.getAndSet(index, null);
                if (command != null) {
                    return command;
                }
            }
            throw CommandPoolExhaustedException.INSTANCE;
        }

        private void release(MutableOrderCommand command) {
            int start = releaseCursor.getAndIncrement();
            int length = pool.length();
            for (int probe = 0; probe < length; probe++) {
                int index = positiveMod(start + probe, length);
                if (pool.compareAndSet(index, null, command)) {
                    return;
                }
            }
            throw CommandPoolExhaustedException.INSTANCE;
        }

        private static int positiveMod(int value, int divisor) {
            return (value & Integer.MAX_VALUE) % divisor;
        }
    }

    private static final class CommandPoolExhaustedException extends RuntimeException {
        private static final CommandPoolExhaustedException INSTANCE = new CommandPoolExhaustedException();

        private CommandPoolExhaustedException() {
            super("NewOrderCommand pool exhausted; increase RISK_RING_CAPACITY", null, false, false);
        }

        @Override
        public synchronized Throwable fillInStackTrace() {
            return this;
        }
    }

    private List<ExchangeEvent> awaitStageOneResult(StageOneCommandEvent event, long sequence, OrderCommand rawCommand) {
        int spins = 0;
        while (!event.isComplete(sequence)) {
            if (spins < 256) {
                Thread.onSpinWait();
            } else if (spins < 1_024) {
                Thread.yield();
            } else {
                LockSupport.parkNanos(1_000L);
            }
            spins++;
        }
        Throwable error = event.error();
        if (error != null) {
            String message = error.getMessage() == null ? "Account risk core failed" : error.getMessage();
            boolean overload = isCapacityFailure(message);
            return List.of(new OrderRejected(rawCommand.sequenceNumber(), rawCommand.orderId(), rawCommand.clientId(),
                    rawCommand.symbol(), overload ? RejectReason.RISK_KILL_SWITCH : RejectReason.INVALID_ORDER_ID,
                    overload ? "Account risk core is overloaded: " + message : message, rawCommand.inboundTimestamp()));
        }
        return event.result();
    }

    private static boolean isCapacityFailure(String message) {
        String lower = message.toLowerCase(java.util.Locale.ROOT);
        return lower.contains("pool exhausted")
                || lower.contains("overloaded")
                || lower.contains("capacity")
                || lower.contains("ring is full");
    }

    private void awaitStageOneApply(StageOneCommandEvent event, long sequence) {
        int spins = 0;
        while (!event.isComplete(sequence)) {
            if (spins < 256) {
                Thread.onSpinWait();
            } else if (spins < 1_024) {
                Thread.yield();
            } else {
                LockSupport.parkNanos(1_000L);
            }
            spins++;
        }
    }

    private final class StageOneRiskHandler implements EventHandler<StageOneCommandEvent> {
        @Override
        public void onEvent(StageOneCommandEvent event, long sequence, boolean endOfBatch) {
            try {
                if (event.mode == StageOneCommandEvent.MODE_APPLY_EVENTS) {
                    if (event.mutableEventsActive) {
                        for (int i = 0; i < event.mutableEvents.size(); i++) {
                            riskEngine.onEvent(event.mutableEvents.get(i));
                        }
                        dispatcher.publish(event.mutableEvents);
                    } else {
                        riskEngine.onEvents(event.events);
                        dispatcher.publish(event.events);
                    }
                    event.complete(EMPTY_EVENTS, null, sequence);
                    return;
                }
                OrderCommand command = event.command();
                List<ExchangeEvent> result = event.asyncMatching
                        ? submitAsyncOnRiskStage(command, event)
                        : submitSyncOnRiskStage(command, event);
                event.complete(result, null, sequence);
            } catch (Throwable t) {
                event.complete(EMPTY_EVENTS, t, sequence);
            }
        }
    }
    private static final class StageOneCommandEvent {
        private static final byte MODE_COMMAND = 1;
        private static final byte MODE_APPLY_EVENTS = 2;

        private final MutableOrderCommand newOrder = new MutableOrderCommand();
        private final MutableCancelOrderCommand cancelOrder = new MutableCancelOrderCommand();
        private final MutableModifyOrderCommand modifyOrder = new MutableModifyOrderCommand();
        private final MutableAdminCommand adminCommand = new MutableAdminCommand();
        private final SingleEventList singleEventResult = new SingleEventList();
        private volatile long completedSequence = Long.MIN_VALUE;
        private volatile List<ExchangeEvent> result = EMPTY_EVENTS;
        private volatile Throwable error;
        private byte mode;
        private boolean asyncMatching;
        private CommandType commandType;
        private long sequenceNumber;
        private java.time.Instant inboundTimestamp = java.time.Instant.EPOCH;
        private String orderId;
        private String clientId;
        private String symbol;
        private int symbolId;
        private Side side;
        private OrderType orderType;
        private Price price;
        private int quantity;
        private SelfTradePreventionMode stpMode;
        private AdminOperation adminOperation;
        private Price newPrice;
        private int newQuantity;
        private long ingressTimeNs;
        private List<ExchangeEvent> events = EMPTY_EVENTS;
        private final MutableEventBatch mutableEvents = new MutableEventBatch(64);
        private boolean mutableEventsActive;

        private void copyCommand(OrderCommand command, boolean asyncMatching, long ringSequence) {
            completedSequence = Long.MIN_VALUE;
            result = EMPTY_EVENTS;
            error = null;
            mode = MODE_COMMAND;
            this.asyncMatching = asyncMatching;
            events = EMPTY_EVENTS;
            mutableEvents.reset();
            mutableEventsActive = false;
            commandType = command.commandType();
            sequenceNumber = command.sequenceNumber();
            inboundTimestamp = command.inboundTimestamp();
            orderId = command.orderId();
            clientId = command.clientId();
            symbol = command.symbol();
            symbolId = command.symbolId();
            ingressTimeNs = command.ingressTimeNs();
            side = null;
            orderType = null;
            price = null;
            quantity = 0;
            stpMode = null;
            adminOperation = null;
            newPrice = null;
            newQuantity = 0;

            if (command instanceof NewOrderCommand newOrderCommand) {
                side = newOrderCommand.side();
                orderType = newOrderCommand.orderType();
                price = newOrderCommand.price();
                quantity = newOrderCommand.quantity();
                stpMode = newOrderCommand.stpMode();
            } else if (command instanceof MutableOrderCommand mutable) {
                side = mutable.side();
                orderType = mutable.orderType();
                price = mutable.price();
                quantity = mutable.quantity();
                stpMode = mutable.stpMode();
            } else if (command instanceof AdminCommand admin) {
                adminOperation = admin.operation();
            } else if (command instanceof MutableAdminCommand admin) {
                adminOperation = admin.operation();
            } else if (command instanceof ModifyOrderCommand modify) {
                newPrice = modify.newPrice();
                newQuantity = modify.newQuantity();
            } else if (command instanceof MutableModifyOrderCommand modify) {
                newPrice = modify.newPrice();
                newQuantity = modify.newQuantity();
            }
        }

        private void copyEvents(List<ExchangeEvent> events, long ringSequence) {
            completedSequence = Long.MIN_VALUE;
            result = EMPTY_EVENTS;
            error = null;
            mode = MODE_APPLY_EVENTS;
            this.events = events;
            mutableEvents.reset();
            mutableEventsActive = false;
            asyncMatching = true;
        }

        private void copyEvents(MutableEventBatch events, long ringSequence) {
            completedSequence = Long.MIN_VALUE;
            result = EMPTY_EVENTS;
            error = null;
            mode = MODE_APPLY_EVENTS;
            this.events = EMPTY_EVENTS;
            mutableEvents.copyFrom(events);
            mutableEventsActive = true;
            asyncMatching = true;
        }

        private OrderCommand command() {
            return switch (commandType) {
                case NEW_ORDER -> {
                    newOrder.populate(orderId, clientId, symbol, symbolId, side, orderType, price, quantity, stpMode,
                            ingressTimeNs);
                    yield newOrder.withSequencing(sequenceNumber, inboundTimestamp);
                }
                case CANCEL_ORDER -> {
                    cancelOrder.populate(sequenceNumber, inboundTimestamp, orderId, clientId, symbol, symbolId);
                    yield cancelOrder;
                }
                case MODIFY_ORDER -> {
                    modifyOrder.populate(sequenceNumber, inboundTimestamp, orderId, clientId, symbol, symbolId,
                            newPrice, newQuantity);
                    yield modifyOrder;
                }
                case ADMIN -> {
                    adminCommand.populate(sequenceNumber, inboundTimestamp, orderId, clientId, symbol, symbolId,
                            adminOperation);
                    yield adminCommand;
                }
            };
        }

        private void complete(List<ExchangeEvent> result, Throwable error, long sequence) {
            this.result = result;
            this.error = error;
            this.events = EMPTY_EVENTS;
            completedSequence = sequence;
        }

        private List<ExchangeEvent> singleResult(ExchangeEvent event) {
            singleEventResult.set(event);
            return singleEventResult;
        }

        private boolean isComplete(long sequence) {
            return completedSequence == sequence;
        }

        private List<ExchangeEvent> result() {
            return result;
        }

        private Throwable error() {
            return error;
        }
    }

    private static final class SingleEventList extends AbstractList<ExchangeEvent> {
        private ExchangeEvent event;

        private void set(ExchangeEvent event) {
            this.event = event;
        }

        @Override
        public ExchangeEvent get(int index) {
            if (index != 0) {
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
