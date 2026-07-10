package exchange.replication;

import exchange.clearing.ClearingService;
import exchange.core.DeterministicMatchingEngine;
import exchange.core.Sequencer;
import exchange.marketdata.MarketDataEngine;
import exchange.model.ExchangeEvent;
import exchange.risk.InMemoryRiskEngine;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Passive state machine that follows the primary's replicated sequenced stream.
 *
 * <p>Rejected commands advance sequence continuity but are not applied to the
 * matching core because the primary already denied them before execution.</p>
 */
public final class PassiveReplica {
    private final Set<String> symbols = ConcurrentHashMap.newKeySet();
    private final DeterministicMatchingEngine matchingEngine;
    private final ClearingService clearingService;
    private final MarketDataEngine marketDataEngine;
    private final Sequencer sequencer;
    private long lastAppliedSequence;

    public PassiveReplica(
            Set<String> initialSymbols,
            DeterministicMatchingEngine matchingEngine,
            InMemoryRiskEngine riskEngine,
            MarketDataEngine marketDataEngine,
            Sequencer sequencer) {
        this.symbols.addAll(initialSymbols);
        this.matchingEngine = matchingEngine;
        this.clearingService = new ClearingService(riskEngine, true);
        this.marketDataEngine = marketDataEngine;
        this.sequencer = sequencer;
    }

    public synchronized List<ExchangeEvent> apply(ReplicationRecord record) {
        long expectedSequence = lastAppliedSequence + 1L;
        if (record.sequenceNumber() != expectedSequence) {
            throw new ReplicationGapException(expectedSequence, record.sequenceNumber());
        }
        symbols.add(record.command().symbol());
        matchingEngine.addSymbol(record.command().symbol());
        lastAppliedSequence = record.sequenceNumber();
        sequencer.advanceToAtLeast(lastAppliedSequence + 1L);
        if (!record.acceptedForMatching()) {
            return List.of();
        }

        List<ExchangeEvent> events = matchingEngine.process(record.command());
        clearingService.onEvents(events);
        marketDataEngine.onEvents(events);
        return List.copyOf(events);
    }

    public synchronized List<ExchangeEvent> applyAll(List<ReplicationRecord> records) {
        ArrayList<ExchangeEvent> events = new ArrayList<>();
        for (ReplicationRecord record : records) {
            events.addAll(apply(record));
        }
        return List.copyOf(events);
    }

    public synchronized long lastAppliedSequence() {
        return lastAppliedSequence;
    }
}
