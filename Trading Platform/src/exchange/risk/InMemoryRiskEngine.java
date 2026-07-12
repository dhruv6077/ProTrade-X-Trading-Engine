package exchange.risk;

import exchange.model.ExchangeEvent;
import exchange.dispatch.RingBufferEvent;
import exchange.model.MutableOrderCommand;
import exchange.model.OrderAccepted;
import exchange.model.OrderCancelled;
import exchange.model.OrderRestated;
import exchange.model.OrderState;
import exchange.model.NewOrderCommand;
import exchange.model.OrderCommand;
import exchange.model.OrderExecuted;
import exchange.model.OrderType;
import exchange.model.RejectReason;
import exchange.model.Side;
import Price.Price;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public final class InMemoryRiskEngine implements RiskEngine {
    private static final int DEFAULT_MARKET_COLLAR_BPS = 500;
    private static final int DEFAULT_POOL_SIZE = configuredPoolSize();
    private static final int TRACKING_MAP_CAPACITY = mapCapacity(DEFAULT_POOL_SIZE);

    private final RiskProfile defaultProfile;
    private final Map<String, RiskProfile> profiles = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ClientAccount> accounts = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, CashReservation> reservations = new ConcurrentHashMap<>(TRACKING_MAP_CAPACITY);
    private final ConcurrentHashMap<String, PositionReservation> positionReservations = new ConcurrentHashMap<>(TRACKING_MAP_CAPACITY);
    private final java.util.Set<String> shortSellingEnabledClients = ConcurrentHashMap.newKeySet();
    private final ConcurrentHashMap<String, AtomicLong> bestBidCents = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> bestAskCents = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, RestingRiskOrder> restingByOrderId = new ConcurrentHashMap<>(TRACKING_MAP_CAPACITY);
    private final ConcurrentHashMap<String, AtomicLong> bestOwnBidCents = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> bestOwnAskCents = new ConcurrentHashMap<>();
    private final PreTradeRiskGuard preTradeRiskGuard = new PreTradeRiskGuard();
    private final ArrayDeque<CashReservation> cashReservationPool = new ArrayDeque<>(DEFAULT_POOL_SIZE);
    private final ArrayDeque<RestingRiskOrder> restingRiskOrderPool = new ArrayDeque<>(DEFAULT_POOL_SIZE);
    private final ArrayDeque<PositionReservation> positionReservationPool = new ArrayDeque<>(DEFAULT_POOL_SIZE);
    private final int marketCollarBps;
    private volatile boolean globalKillSwitchEnabled;
    private volatile boolean hardRejectSelfTrade;
    private long lastSettledSequence = Long.MIN_VALUE;
    private long lastSettledPriceCents;
    private int lastSettledQty;
    private String lastSettledOrderId;
    private String lastSettledContraOrderId;

    private static int configuredPoolSize() {
        String configured = System.getProperty("riskObjectPoolSize");
        if (configured == null || configured.isBlank()) {
            configured = System.getenv("RISK_OBJECT_POOL_SIZE");
        }
        return configured == null || configured.isBlank() ? 16_384 : Integer.parseInt(configured);
    }

    private static int mapCapacity(int expectedEntries) {
        return Math.max(16, (int) Math.min(1 << 30, (long) (expectedEntries / 0.75d) + 1L));
    }

    public InMemoryRiskEngine(RiskProfile defaultProfile) {
        this(defaultProfile, DEFAULT_MARKET_COLLAR_BPS);
    }

    public InMemoryRiskEngine(RiskProfile defaultProfile, int marketCollarBps) {
        this.defaultProfile = defaultProfile;
        this.marketCollarBps = marketCollarBps;
        for (int i = 0; i < DEFAULT_POOL_SIZE; i++) {
            cashReservationPool.addLast(new CashReservation());
            restingRiskOrderPool.addLast(new RestingRiskOrder());
            positionReservationPool.addLast(new PositionReservation());
        }
    }

    public void setProfile(String clientId, RiskProfile profile) {
        profiles.put(clientId, profile);
        account(clientId).setAvailableCash(profile.buyingPowerCents());
    }

    public ClientAccount account(String clientId) {
        return accounts.computeIfAbsent(clientId,
                ignored -> new ClientAccount(clientId, profileFor(clientId).buyingPowerCents()));
    }

    public void setAvailableCash(String clientId, long availableCashCents) {
        account(clientId).setAvailableCash(availableCashCents);
    }

    public void setPosition(String clientId, String symbol, long quantity) {
        account(clientId).setPosition(symbol, quantity);
    }

    public void setPosition(String clientId, String symbol, int symbolId, long quantity) {
        account(clientId).setPosition(symbol, symbolId, quantity);
    }

    public void setShortSellingEnabled(String clientId, boolean enabled) {
        if (enabled) {
            shortSellingEnabledClients.add(clientId);
        } else {
            shortSellingEnabledClients.remove(clientId);
        }
    }

    public void setReferencePrice(String symbol, Side liquiditySide, long priceCents) {
        if (liquiditySide == Side.BUY) {
            updateBest(bestBidCents, symbol, priceCents, true);
        } else {
            updateBest(bestAskCents, symbol, priceCents, false);
        }
    }

    public void hydrateAccepted(String orderId, String clientId, String symbol, Side side, exchange.model.OrderType orderType,
            Price price, int quantity, long sequenceNumber) {
        if (side != Side.BUY || price == null || quantity <= 0) {
            if (price != null && quantity > 0) {
                trackRestingOrder(orderId, clientId, symbol, side, orderType, price, quantity, quantity, 0,
                        sequenceNumber);
            }
            return;
        }
        long reserveNotionalCents = Math.multiplyExact(price.getCents(), quantity);
        ClientAccount account = account(clientId);
        account.reserveCash(reserveNotionalCents);
        putOrResetCashReservation(orderId, clientId, symbol, 0, quantity, price.getCents(), reserveNotionalCents);
        trackRestingOrder(orderId, clientId, symbol, side, orderType, price, quantity, quantity, 0, sequenceNumber);
    }

    public List<RestingOrderSnapshot> restingOrdersFor(String clientId) {
        return restingByOrderId.values().stream()
                .filter(order -> order.clientId.equals(clientId) && order.leavesQty > 0)
                .map(order -> new RestingOrderSnapshot(order.orderId, order.symbol, order.side, order.orderType,
                        order.priceCents, order.quantity, order.leavesQty, order.cumQty, order.sequenceNumber))
                .sorted((left, right) -> Long.compare(right.sequenceNumber(), left.sequenceNumber()))
                .toList();
    }

    public void setGlobalKillSwitchEnabled(boolean enabled) {
        this.globalKillSwitchEnabled = enabled;
    }

    public void setHardRejectSelfTrade(boolean enabled) {
        this.hardRejectSelfTrade = enabled;
    }

    public void purgeClient(String clientId) {
        profiles.compute(clientId, (ignored, profile) -> {
            RiskProfile source = profile == null ? defaultProfile : profile;
            return new RiskProfile(source.maxOrderNotionalCents(), source.maxPosition(),
                    source.buyingPowerCents(), true, source.maxPriceDeviationBps());
        });
    }

    @Override
    public RiskDecision check(OrderCommand command) {
        if (globalKillSwitchEnabled) {
            return RiskDecision.GLOBAL_KILL_SWITCH;
        }
        RiskProfile profile = profileFor(command.clientId());
        if (profile.killSwitchEnabled()) {
            return RiskDecision.CLIENT_KILL_SWITCH;
        }
        if (!(command instanceof NewOrderCommand) && !(command instanceof MutableOrderCommand)) {
            return RiskDecision.accept();
        }

        RiskDecision statelessDecision = preTradeRiskGuard.check(command, profile,
                referencePriceCents(command), wouldSelfTrade(command), hardRejectSelfTrade);
        if (!statelessDecision.accepted()) {
            return statelessDecision;
        }

        long reserveNotionalCents = reserveNotional(command);
        if (reserveNotionalCents < 0) {
            return RiskDecision.MARKET_REQUIRES_REFERENCE;
        }
        if (reserveNotionalCents > profile.maxOrderNotionalCents()) {
            return RiskDecision.ORDER_NOTIONAL_LIMIT;
        }

        Side side = side(command);
        int quantity = quantity(command);
        String clientId = command.clientId();
        String symbol = command.symbol();
        int symbolId = command.symbolId();
        ClientAccount account = account(clientId);
        long currentPosition = symbolId > 0 ? account.position(symbolId) : account.position(symbol);
        if (symbolId > 0 && currentPosition == 0L) {
            currentPosition = account.position(symbol);
            if (currentPosition != 0L) {
                account.setPosition(symbol, symbolId, currentPosition);
            }
        }
        if (RiskMath.exceedsPositionLimit(currentPosition, side, quantity, profile.maxPosition())) {
            return RiskDecision.PROJECTED_POSITION_LIMIT;
        }

        if (side == Side.BUY) {
            CashReservation reservation = borrowCashReservation();
            if (!account.reserveCash(reserveNotionalCents)) {
                recycle(reservation);
                return RiskDecision.INSUFFICIENT_AVAILABLE_CASH;
            }
            putOrResetCashReservation(command.orderId(), reservation, clientId, symbol, symbolId, quantity, reserveUnitCents(command),
                    reserveNotionalCents);
        } else if (!shortSellingEnabledClients.contains(clientId)) {
            if (!account.reservePosition(symbol, symbolId, quantity)) {
                return RiskDecision.INSUFFICIENT_POSITION;
            }
            PositionReservation reservation = borrowPositionReservation();
            reservation.reset(command.orderId(), clientId, symbol, symbolId, quantity);
            positionReservations.put(command.orderId(), reservation);
        }

        return RiskDecision.accept();
    }

    private RiskProfile profileFor(String clientId) {
        return profiles.getOrDefault(clientId, defaultProfile);
    }

    private long reserveNotional(OrderCommand order) {
        if (side(order) == Side.SELL) {
            return 0;
        }
        long reserveUnitCents = reserveUnitCents(order);
        if (reserveUnitCents < 0) {
            return -1;
        }
        return RiskMath.multiplyPositiveOrMax(reserveUnitCents, quantity(order));
    }

    private long reserveUnitCents(OrderCommand order) {
        if (orderType(order) == OrderType.MARKET) {
            long askCents = bestValue(bestAskCents, order.symbol(), -1L);
            if (askCents <= 0L) {
                return -1;
            }
            return applyCollar(askCents);
        }
        if (price(order) == null) {
            return -1;
        }
        return price(order).getCents();
    }

    private long applyCollar(long referenceCents) {
        return RiskMath.multiplyPositiveOrMax(referenceCents, 10_000L + marketCollarBps) / 10_000L;
    }

    private long referencePriceCents(OrderCommand command) {
        long primary = side(command) == Side.BUY
                ? bestValue(bestAskCents, command.symbol(), -1L)
                : bestValue(bestBidCents, command.symbol(), -1L);
        if (primary > 0L) {
            return primary;
        }
        return side(command) == Side.BUY
                ? bestValue(bestBidCents, command.symbol(), -1L)
                : bestValue(bestAskCents, command.symbol(), -1L);
    }

    private boolean wouldSelfTrade(OrderCommand command) {
        Price commandPrice = price(command);
        String key = ownerSymbolKey(command.clientId(), command.symbol());
        if (side(command) == Side.BUY) {
            long ownBestAsk = bestValue(bestOwnAskCents, key, -1L);
            return ownBestAsk > 0L && priceCrosses(Side.BUY, commandPrice, ownBestAsk);
        }
        long ownBestBid = bestValue(bestOwnBidCents, key, -1L);
        return ownBestBid > 0L && priceCrosses(Side.SELL, commandPrice, ownBestBid);
    }

    private boolean priceCrosses(Side side, Price aggressivePrice, long passivePriceCents) {
        if (aggressivePrice == null) {
            return true;
        }
        long aggressiveCents = aggressivePrice.getCents();
        return side == Side.BUY ? aggressiveCents >= passivePriceCents : aggressiveCents <= passivePriceCents;
    }

    @Override
    public void onEvents(List<ExchangeEvent> events) {
        for (ExchangeEvent event : events) {
            if (event instanceof OrderAccepted accepted && accepted.order().price() != null) {
                setReferencePrice(accepted.symbol(), accepted.order().side(), accepted.order().price().getCents());
                trackAccepted(accepted);
            } else if (event instanceof OrderExecuted executed) {
                setReferencePrice(executed.symbol(), executed.side(), executed.fillPrice().getCents());
                settleExecution(executed);
                trackExecution(executed);
            } else if (event instanceof OrderCancelled cancelled) {
                releaseReservation(cancelled.orderId(), cancelled.cancelledQty());
                trackCancel(cancelled);
            } else if (event instanceof exchange.model.OrderRejected rejected) {
                releaseReservation(rejected.orderId());
            } else if (event instanceof OrderRestated restated) {
                trackRestated(restated);
            }
        }
    }

    @Override
    public void onEvent(RingBufferEvent event) {
        switch (event.getEventType()) {
            case ACCEPTED -> {
                if (event.getPrice() != null) {
                    setReferencePrice(event.getSymbol(), event.getSide(), event.getPrice().getCents());
                    if (event.getOrderType() == OrderType.LIMIT && event.getLeavesQty() > 0) {
                        trackRestingOrder(event.getOrderId(), event.getClientId(), event.getSymbol(),
                                event.getSide(), event.getOrderType(), event.getPrice(), event.getQuantity(),
                                event.getLeavesQty(), event.getCumQty(), event.getSequenceNumber());
                    }
                }
            }
            case EXECUTED -> {
                setReferencePrice(event.getSymbol(), event.getSide(), event.getFillPrice().getCents());
                settleExecution(event);
                trackExecution(event);
            }
            case CANCELLED -> {
                releaseReservation(event.getOrderId(), event.getCancelledQty());
                trackCancel(event);
            }
            case REJECTED -> releaseReservation(event.getOrderId());
            case RESTATED -> trackRestated(event);
            default -> {
            }
        }
    }

    private void settleExecution(OrderExecuted executed) {
        if (isDuplicateSettlement(executed.sequenceNumber(), executed.orderId(), executed.contraOrderId(),
                executed.fillPrice().getCents(), executed.fillQty())) {
            return;
        }
        boolean eventClientIsBuyer = executed.side() == Side.BUY;
        String buyerOrderId = eventClientIsBuyer ? executed.orderId() : executed.contraOrderId();
        String buyerClientId = eventClientIsBuyer ? executed.clientId() : executed.contraClientId();
        String sellerOrderId = eventClientIsBuyer ? executed.contraOrderId() : executed.orderId();
        String sellerClientId = eventClientIsBuyer ? executed.contraClientId() : executed.clientId();
        long notionalCents = Math.multiplyExact(executed.fillPrice().getCents(), executed.fillQty());

        settleBuyerFill(buyerOrderId, buyerClientId, executed.symbol(), executed.fillQty(), notionalCents);
        creditSellerFill(sellerOrderId, sellerClientId, executed.symbol(), executed.fillQty(), notionalCents);
    }

    private void settleExecution(RingBufferEvent executed) {
        if (isDuplicateSettlement(executed.getSequenceNumber(), executed.getOrderId(), executed.getContraOrderId(),
                executed.getFillPrice().getCents(), executed.getFillQty())) {
            return;
        }
        boolean eventClientIsBuyer = executed.getSide() == Side.BUY;
        String buyerOrderId = eventClientIsBuyer ? executed.getOrderId() : executed.getContraOrderId();
        String buyerClientId = eventClientIsBuyer ? executed.getClientId() : executed.getContraClientId();
        String sellerOrderId = eventClientIsBuyer ? executed.getContraOrderId() : executed.getOrderId();
        String sellerClientId = eventClientIsBuyer ? executed.getContraClientId() : executed.getClientId();
        long notionalCents = Math.multiplyExact(executed.getFillPrice().getCents(), executed.getFillQty());

        settleBuyerFill(buyerOrderId, buyerClientId, executed.getSymbol(), executed.getFillQty(), notionalCents);
        creditSellerFill(sellerOrderId, sellerClientId, executed.getSymbol(), executed.getFillQty(), notionalCents);
    }

    private boolean isDuplicateSettlement(long sequenceNumber, String orderId, String contraOrderId,
            long priceCents, int fillQty) {
        boolean duplicate = sequenceNumber == lastSettledSequence
                && priceCents == lastSettledPriceCents
                && fillQty == lastSettledQty
                && orderId.equals(lastSettledContraOrderId)
                && contraOrderId.equals(lastSettledOrderId);
        lastSettledSequence = sequenceNumber;
        lastSettledPriceCents = priceCents;
        lastSettledQty = fillQty;
        lastSettledOrderId = orderId;
        lastSettledContraOrderId = contraOrderId;
        return duplicate;
    }

    private void trackAccepted(OrderAccepted accepted) {
        OrderState order = accepted.order();
        if (order.orderType() != OrderType.LIMIT || order.price() == null || order.leavesQty() <= 0) {
            return;
        }
        trackRestingOrder(order.orderId(), order.clientId(), order.symbol(), order.side(), order.orderType(),
                order.price(), order.quantity(), order.leavesQty(), order.cumQty(), order.sequenceNumber());
    }

    private void trackRestingOrder(String orderId, String clientId, String symbol, Side side, OrderType orderType,
            Price price, int quantity, int leavesQty, int cumQty, long sequenceNumber) {
        RestingRiskOrder resting = restingByOrderId.get(orderId);
        if (resting == null) {
            resting = borrowRestingRiskOrder();
            RestingRiskOrder existing = restingByOrderId.putIfAbsent(orderId, resting);
            if (existing != null) {
                recycle(resting);
                resting = existing;
            }
        }
        resting.reset(orderId, clientId, symbol, side, orderType, price.getCents(), quantity, leavesQty, cumQty,
                sequenceNumber);
        updateOwnBest(resting);
    }

    private void putOrResetCashReservation(String orderId, String clientId, String symbol, int symbolId, long quantity,
            long reserveUnitCents, long totalReservedCents) {
        putOrResetCashReservation(orderId, borrowCashReservation(), clientId, symbol, symbolId, quantity, reserveUnitCents,
                totalReservedCents);
    }

    private void putOrResetCashReservation(String orderId, CashReservation borrowed, String clientId, String symbol,
            int symbolId, long quantity, long reserveUnitCents, long totalReservedCents) {
        CashReservation reservation = reservations.get(orderId);
        if (reservation == null) {
            reservation = borrowed;
            CashReservation existing = reservations.putIfAbsent(orderId, borrowed);
            if (existing != null) {
                recycle(borrowed);
                reservation = existing;
            }
        } else {
            recycle(borrowed);
        }
        reservation.reset(orderId, clientId, symbol, symbolId, quantity, reserveUnitCents, totalReservedCents);
    }

    private CashReservation borrowCashReservation() {
        CashReservation reservation = cashReservationPool.pollFirst();
        if (reservation == null) {
            throw PoolExhaustedException.CASH_RESERVATION;
        }
        return reservation;
    }

    private RestingRiskOrder borrowRestingRiskOrder() {
        RestingRiskOrder resting = restingRiskOrderPool.pollFirst();
        if (resting == null) {
            throw PoolExhaustedException.RESTING_RISK_ORDER;
        }
        return resting;
    }

    private PositionReservation borrowPositionReservation() {
        PositionReservation reservation = positionReservationPool.pollFirst();
        if (reservation == null) {
            throw PoolExhaustedException.POSITION_RESERVATION;
        }
        return reservation;
    }

    private void recycle(CashReservation reservation) {
        reservation.clear();
        if (cashReservationPool.size() < DEFAULT_POOL_SIZE) {
            cashReservationPool.addLast(reservation);
        }
    }

    private void recycle(RestingRiskOrder resting) {
        resting.clear();
        if (restingRiskOrderPool.size() < DEFAULT_POOL_SIZE) {
            restingRiskOrderPool.addLast(resting);
        }
    }

    private void recycle(PositionReservation reservation) {
        reservation.clear();
        if (positionReservationPool.size() < DEFAULT_POOL_SIZE) {
            positionReservationPool.addLast(reservation);
        }
    }

    private void trackExecution(OrderExecuted executed) {
        RestingRiskOrder resting = restingByOrderId.get(executed.orderId());
        if (resting == null) {
            return;
        }
        String clientId = resting.clientId;
        String symbol = resting.symbol;
        Side side = resting.side;
        if (executed.leavesQty() <= 0 || executed.fullFill()) {
            if (restingByOrderId.remove(executed.orderId(), resting)) {
                recycle(resting);
            }
        } else {
            resting.leavesQty = executed.leavesQty();
            resting.cumQty = executed.cumQty();
        }
        recomputeOwnBest(clientId, symbol, side);
    }

    private void trackExecution(RingBufferEvent executed) {
        RestingRiskOrder resting = restingByOrderId.get(executed.getOrderId());
        if (resting == null) {
            return;
        }
        String clientId = resting.clientId;
        String symbol = resting.symbol;
        Side side = resting.side;
        if (executed.getLeavesQty() <= 0 || executed.isFullFill()) {
            if (restingByOrderId.remove(executed.getOrderId(), resting)) {
                recycle(resting);
            }
        } else {
            resting.leavesQty = executed.getLeavesQty();
            resting.cumQty = executed.getCumQty();
        }
        recomputeOwnBest(clientId, symbol, side);
    }

    private void trackCancel(OrderCancelled cancelled) {
        RestingRiskOrder resting = restingByOrderId.get(cancelled.orderId());
        if (resting == null) {
            return;
        }
        String clientId = resting.clientId;
        String symbol = resting.symbol;
        Side side = resting.side;
        if (cancelled.cancelledQty() >= resting.leavesQty) {
            if (restingByOrderId.remove(cancelled.orderId(), resting)) {
                recycle(resting);
            }
        } else {
            resting.leavesQty -= cancelled.cancelledQty();
        }
        recomputeOwnBest(clientId, symbol, side);
    }

    private void trackCancel(RingBufferEvent cancelled) {
        RestingRiskOrder resting = restingByOrderId.get(cancelled.getOrderId());
        if (resting == null) {
            return;
        }
        String clientId = resting.clientId;
        String symbol = resting.symbol;
        Side side = resting.side;
        if (cancelled.getCancelledQty() >= resting.leavesQty) {
            if (restingByOrderId.remove(cancelled.getOrderId(), resting)) {
                recycle(resting);
            }
        } else {
            resting.leavesQty -= cancelled.getCancelledQty();
        }
        recomputeOwnBest(clientId, symbol, side);
    }

    private void trackRestated(OrderRestated restated) {
        RestingRiskOrder resting = restingByOrderId.get(restated.orderId());
        if (resting == null) {
            return;
        }
        Side side = resting.side;
        String clientId = resting.clientId;
        String symbol = resting.symbol;
        if (restated.leavesQty() <= 0 || restated.price() == null) {
            if (restingByOrderId.remove(restated.orderId(), resting)) {
                recycle(resting);
            }
        } else {
            resting.priceCents = restated.price().getCents();
            resting.quantity = restated.quantity();
            resting.leavesQty = restated.leavesQty();
            resting.cumQty = restated.cumQty();
            resting.sequenceNumber = restated.sequenceNumber();
        }
        recomputeOwnBest(clientId, symbol, side);
    }

    private void trackRestated(RingBufferEvent restated) {
        RestingRiskOrder resting = restingByOrderId.get(restated.getOrderId());
        if (resting == null) {
            return;
        }
        Side side = resting.side;
        String clientId = resting.clientId;
        String symbol = resting.symbol;
        if (restated.getLeavesQty() <= 0 || restated.getPrice() == null) {
            if (restingByOrderId.remove(restated.getOrderId(), resting)) {
                recycle(resting);
            }
        } else {
            resting.priceCents = restated.getPrice().getCents();
            resting.quantity = restated.getQuantity();
            resting.leavesQty = restated.getLeavesQty();
            resting.cumQty = restated.getCumQty();
            resting.sequenceNumber = restated.getSequenceNumber();
        }
        recomputeOwnBest(clientId, symbol, side);
    }

    private void updateOwnBest(RestingRiskOrder resting) {
        String key = ownerSymbolKey(resting.clientId, resting.symbol);
        if (resting.side == Side.BUY) {
            updateBest(bestOwnBidCents, key, resting.priceCents, true);
        } else {
            updateBest(bestOwnAskCents, key, resting.priceCents, false);
        }
    }

    private void recomputeOwnBest(String clientId, String symbol, Side side) {
        String key = ownerSymbolKey(clientId, symbol);
        long best = side == Side.BUY ? Long.MIN_VALUE : Long.MAX_VALUE;
        for (RestingRiskOrder resting : restingByOrderId.values()) {
            if (resting.leavesQty <= 0
                    || resting.side != side
                    || !resting.clientId.equals(clientId)
                    || !resting.symbol.equals(symbol)) {
                continue;
            }
            best = side == Side.BUY ? Math.max(best, resting.priceCents) : Math.min(best, resting.priceCents);
        }
        if (side == Side.BUY) {
            if (best == Long.MIN_VALUE) {
                bestOwnBidCents.remove(key);
            } else {
                atomicCell(bestOwnBidCents, key, best).set(best);
            }
        } else if (best == Long.MAX_VALUE) {
            bestOwnAskCents.remove(key);
        } else {
            atomicCell(bestOwnAskCents, key, best).set(best);
        }
    }

    private static long bestValue(ConcurrentHashMap<String, AtomicLong> map, String key, long missingValue) {
        AtomicLong value = map.get(key);
        return value == null ? missingValue : value.get();
    }

    private static void updateBest(ConcurrentHashMap<String, AtomicLong> map, String key, long candidate,
            boolean maximum) {
        AtomicLong value = atomicCell(map, key, candidate);
        while (true) {
            long current = value.get();
            long updated = maximum ? Math.max(current, candidate) : Math.min(current, candidate);
            if (updated == current || value.compareAndSet(current, updated)) {
                return;
            }
        }
    }

    private static AtomicLong atomicCell(ConcurrentHashMap<String, AtomicLong> map, String key, long initialValue) {
        AtomicLong value = map.get(key);
        if (value != null) {
            return value;
        }
        AtomicLong created = new AtomicLong(initialValue);
        AtomicLong existing = map.putIfAbsent(key, created);
        return existing == null ? created : existing;
    }

    private String ownerSymbolKey(String clientId, String symbol) {
        return clientId + '\u0001' + symbol;
    }

    @Override
    public void applyFill(NewOrderCommand aggressiveOrder, int signedQuantity, long notionalCents) {
    }

    private Side side(OrderCommand command) {
        return command instanceof NewOrderCommand newOrder ? newOrder.side() : ((MutableOrderCommand) command).side();
    }

    private OrderType orderType(OrderCommand command) {
        return command instanceof NewOrderCommand newOrder
                ? newOrder.orderType()
                : ((MutableOrderCommand) command).orderType();
    }

    private int quantity(OrderCommand command) {
        return command instanceof NewOrderCommand newOrder
                ? newOrder.quantity()
                : ((MutableOrderCommand) command).quantity();
    }

    private Price price(OrderCommand command) {
        return command instanceof NewOrderCommand newOrder ? newOrder.price() : ((MutableOrderCommand) command).price();
    }

    public void settleBuyerFill(String orderId, String clientId, String symbol, int fillQty, long fillNotionalCents) {
        CashReservation reservation = reservations.get(orderId);
        if (reservation == null) {
            ClientAccount account = account(clientId);
            if (account.reserveCash(fillNotionalCents)) {
                account.settleReservedCash(fillNotionalCents, 0);
            }
            account.addPosition(symbol, 0, fillQty);
            return;
        }

        long reservedDebit = reservation.consumeForFill(fillQty, fillNotionalCents);
        long unusedReserve = Math.max(0, reservedDebit - fillNotionalCents);
        account(clientId).settleReservedCash(reservedDebit, unusedReserve);
        account(clientId).addPosition(symbol, reservation.symbolId(), fillQty);
        if (reservation.isEmpty() && reservations.remove(orderId, reservation)) {
            recycle(reservation);
        }
    }

    public void creditSellerFill(String orderId, String clientId, String symbol, int fillQty, long fillNotionalCents) {
        int symbolId = consumeSellerReservation(orderId, fillQty);
        ClientAccount account = account(clientId);
        account.creditCash(fillNotionalCents);
        account.addPosition(symbol, symbolId, -fillQty);
    }

    public void releaseReservation(String orderId) {
        CashReservation reservation = reservations.remove(orderId);
        if (reservation != null) {
            account(reservation.clientId()).releaseReservedCash(reservation.releaseAll());
            recycle(reservation);
        }
        PositionReservation positionReservation = positionReservations.remove(orderId);
        if (positionReservation != null) {
            account(positionReservation.clientId()).releaseReservedPosition(positionReservation.symbol(),
                    positionReservation.symbolId(), positionReservation.releaseAll());
            recycle(positionReservation);
        }
    }

    public void releaseReservation(String orderId, int quantity) {
        CashReservation reservation = reservations.get(orderId);
        if (reservation != null) {
            long released = reservation.releaseQuantity(quantity);
            account(reservation.clientId()).releaseReservedCash(released);
            if (reservation.isEmpty() && reservations.remove(orderId, reservation)) {
                recycle(reservation);
            }
        }
        PositionReservation positionReservation = positionReservations.get(orderId);
        if (positionReservation != null) {
            long released = positionReservation.releaseQuantity(quantity);
            account(positionReservation.clientId()).releaseReservedPosition(positionReservation.symbol(),
                    positionReservation.symbolId(), released);
            if (positionReservation.isEmpty()) {
                if (positionReservations.remove(orderId, positionReservation)) {
                    recycle(positionReservation);
                }
            }
        }
    }

    private int consumeSellerReservation(String orderId, int fillQty) {
        PositionReservation reservation = positionReservations.get(orderId);
        if (reservation == null) {
            return 0;
        }
        int symbolId = reservation.symbolId();
        long consumed = reservation.releaseQuantity(fillQty);
        account(reservation.clientId()).releaseReservedPosition(reservation.symbol(), reservation.symbolId(), consumed);
        if (reservation.isEmpty()) {
            if (positionReservations.remove(orderId, reservation)) {
                recycle(reservation);
            }
        }
        return symbolId;
    }

    private static final class CashReservation {
        private String orderId;
        private String clientId;
        private String symbol;
        private int symbolId;
        private long reserveUnitCents;
        private long remainingQuantity;
        private long remainingCashCents;

        private void reset(String orderId, String clientId, String symbol, int symbolId, long quantity,
                long reserveUnitCents, long totalReservedCents) {
            this.orderId = orderId;
            this.clientId = clientId;
            this.symbol = symbol;
            this.symbolId = symbolId;
            this.reserveUnitCents = reserveUnitCents;
            this.remainingQuantity = quantity;
            this.remainingCashCents = totalReservedCents;
        }

        private void clear() {
            orderId = null;
            clientId = null;
            symbol = null;
            symbolId = 0;
            reserveUnitCents = 0L;
            remainingQuantity = 0L;
            remainingCashCents = 0L;
        }

        private String clientId() {
            return clientId;
        }

        private int symbolId() {
            return symbolId;
        }

        private long consumeForFill(long fillQty, long fillNotionalCents) {
            remainingQuantity -= fillQty;
            return releaseCash(Math.max(fillNotionalCents, Math.multiplyExact(reserveUnitCents, fillQty)));
        }

        private long releaseQuantity(long quantity) {
            remainingQuantity -= quantity;
            return releaseCash(Math.multiplyExact(reserveUnitCents, quantity));
        }

        private long releaseAll() {
            remainingQuantity = 0;
            long released = remainingCashCents;
            remainingCashCents = 0;
            return released;
        }

        private long releaseCash(long requestedCents) {
            long released = Math.min(remainingCashCents, requestedCents);
            remainingCashCents -= released;
            return released;
        }

        private boolean isEmpty() {
            return remainingQuantity <= 0 || remainingCashCents <= 0;
        }

        @Override
        public String toString() {
            return orderId + ":" + clientId + ":" + symbol;
        }
    }

    public record RestingOrderSnapshot(String orderId, String symbol, Side side, OrderType orderType, long priceCents,
            int quantity, long leavesQty, int cumQty, long sequenceNumber) {
    }

    private static final class PoolExhaustedException extends RuntimeException {
        private static final PoolExhaustedException CASH_RESERVATION = new PoolExhaustedException(
                "CashReservation pool exhausted; increase RISK_OBJECT_POOL_SIZE");
        private static final PoolExhaustedException RESTING_RISK_ORDER = new PoolExhaustedException(
                "RestingRiskOrder pool exhausted; increase RISK_OBJECT_POOL_SIZE");
        private static final PoolExhaustedException POSITION_RESERVATION = new PoolExhaustedException(
                "PositionReservation pool exhausted; increase RISK_OBJECT_POOL_SIZE");

        private PoolExhaustedException(String message) {
            super(message, null, false, false);
        }

        @Override
        public synchronized Throwable fillInStackTrace() {
            return this;
        }
    }

    private static final class RestingRiskOrder {
        private String orderId;
        private String clientId;
        private String symbol;
        private Side side;
        private OrderType orderType;
        private long priceCents;
        private int quantity;
        private long leavesQty;
        private int cumQty;
        private long sequenceNumber;

        private void reset(String orderId, String clientId, String symbol, Side side, OrderType orderType,
                long priceCents, int quantity, long leavesQty, int cumQty, long sequenceNumber) {
            this.orderId = orderId;
            this.clientId = clientId;
            this.symbol = symbol;
            this.side = side;
            this.orderType = orderType;
            this.priceCents = priceCents;
            this.quantity = quantity;
            this.leavesQty = leavesQty;
            this.cumQty = cumQty;
            this.sequenceNumber = sequenceNumber;
        }

        private void clear() {
            orderId = null;
            clientId = null;
            symbol = null;
            side = null;
            orderType = null;
            priceCents = 0L;
            quantity = 0;
            leavesQty = 0L;
            cumQty = 0;
            sequenceNumber = 0L;
        }
    }

    private static final class PositionReservation {
        private String orderId;
        private String clientId;
        private String symbol;
        private int symbolId;
        private long remainingQuantity;

        private void reset(String orderId, String clientId, String symbol, int symbolId, long quantity) {
            this.orderId = orderId;
            this.clientId = clientId;
            this.symbol = symbol;
            this.symbolId = symbolId;
            this.remainingQuantity = quantity;
        }

        private void clear() {
            orderId = null;
            clientId = null;
            symbol = null;
            symbolId = 0;
            remainingQuantity = 0L;
        }

        private String clientId() {
            return clientId;
        }

        private String symbol() {
            return symbol;
        }

        private int symbolId() {
            return symbolId;
        }

        private long releaseQuantity(long quantity) {
            long released = Math.min(remainingQuantity, quantity);
            remainingQuantity -= released;
            return released;
        }

        private long releaseAll() {
            long released = remainingQuantity;
            remainingQuantity = 0L;
            return released;
        }

        private boolean isEmpty() {
            return remainingQuantity <= 0;
        }

        @Override
        public String toString() {
            return orderId + ":" + clientId + ":" + symbol;
        }
    }
}
