package exchange.clearing;

import com.lmax.disruptor.EventHandler;
import exchange.dispatch.EventListener;
import exchange.dispatch.RingBufferEvent;
import exchange.model.ExchangeEvent;
import exchange.model.OrderAccepted;
import exchange.model.OrderCancelled;
import exchange.model.OrderExecuted;
import exchange.model.OrderRejected;
import exchange.model.Side;
import exchange.risk.InMemoryRiskEngine;

import java.util.List;

public final class ClearingService implements EventHandler<RingBufferEvent>, EventListener {
    private final InMemoryRiskEngine riskEngine;
    private final boolean hydrateAcceptedReservations;
    private long lastSettledSequence = Long.MIN_VALUE;
    private long lastSettledPriceCents;
    private int lastSettledQty;
    private String lastSettledOrderId;
    private String lastSettledContraOrderId;

    public ClearingService(InMemoryRiskEngine riskEngine) {
        this(riskEngine, false);
    }

    public ClearingService(InMemoryRiskEngine riskEngine, boolean hydrateAcceptedReservations) {
        this.riskEngine = riskEngine;
        this.hydrateAcceptedReservations = hydrateAcceptedReservations;
    }

    @Override
    public void onEvent(RingBufferEvent event, long sequence, boolean endOfBatch) {
        if (!hydrateAcceptedReservations) {
            riskEngine.onEvents(List.of(event.toImmutableEvent()));
            return;
        }
        apply(event);
    }

    @Override
    public void onEvents(List<ExchangeEvent> events) {
        if (!hydrateAcceptedReservations) {
            riskEngine.onEvents(events);
            return;
        }
        for (ExchangeEvent event : events) {
            apply(event);
        }
    }

    private void apply(RingBufferEvent event) {
        switch (event.getEventType()) {
            case ACCEPTED -> {
                if (hydrateAcceptedReservations) {
                    riskEngine.hydrateAccepted(event.getOrderId(), event.getClientId(), event.getSymbol(),
                            event.getSide(), event.getOrderType(), event.getPrice(), event.getLeavesQty(),
                            event.getSequenceNumber());
                }
            }
            case EXECUTED -> settleExecution(event.getOrderId(), event.getClientId(), event.getContraOrderId(),
                    event.getContraClientId(), event.getSide(), event.getSymbol(), event.getSequenceNumber(),
                    event.getFillPrice().getCents(), event.getFillQty());
            case CANCELLED -> riskEngine.releaseReservation(event.getOrderId(), event.getCancelledQty());
            case REJECTED -> riskEngine.releaseReservation(event.getOrderId());
            default -> {
            }
        }
    }

    private void apply(ExchangeEvent event) {
        if (hydrateAcceptedReservations && event instanceof OrderAccepted accepted) {
            riskEngine.hydrateAccepted(accepted.orderId(), accepted.clientId(), accepted.symbol(),
                    accepted.order().side(), accepted.order().orderType(), accepted.order().price(),
                    accepted.order().leavesQty(), accepted.sequenceNumber());
        } else if (event instanceof OrderExecuted executed) {
            settleExecution(executed.orderId(), executed.clientId(), executed.contraOrderId(),
                    executed.contraClientId(), executed.side(), executed.symbol(), executed.sequenceNumber(),
                    executed.fillPrice().getCents(), executed.fillQty());
        } else if (event instanceof OrderCancelled cancelled) {
            riskEngine.releaseReservation(cancelled.orderId(), cancelled.cancelledQty());
        } else if (event instanceof OrderRejected rejected) {
            riskEngine.releaseReservation(rejected.orderId());
        }
    }

    private void settleExecution(String orderId, String clientId, String contraOrderId, String contraClientId,
            Side side, String symbol, long sequenceNumber, long fillPriceCents, int fillQty) {
        if (isDuplicateSettlement(sequenceNumber, orderId, contraOrderId, fillPriceCents, fillQty)) {
            return;
        }

        boolean eventClientIsBuyer = side == Side.BUY;
        String buyerOrderId = eventClientIsBuyer ? orderId : contraOrderId;
        String buyerClientId = eventClientIsBuyer ? clientId : contraClientId;
        String sellerOrderId = eventClientIsBuyer ? contraOrderId : orderId;
        String sellerClientId = eventClientIsBuyer ? contraClientId : clientId;
        long notionalCents = Math.multiplyExact(fillPriceCents, fillQty);

        riskEngine.settleBuyerFill(buyerOrderId, buyerClientId, symbol, fillQty, notionalCents);
        riskEngine.creditSellerFill(sellerOrderId, sellerClientId, symbol, fillQty, notionalCents);
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
}
