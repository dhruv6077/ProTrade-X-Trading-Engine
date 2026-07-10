package exchange;

import Price.PriceFactory;
import exchange.core.DeterministicMatchingEngine;
import exchange.core.Sequencer;
import exchange.dispatch.InMemoryEventDispatcher;
import exchange.gateway.GatewayValidator;
import exchange.gateway.OrderGateway;
import exchange.journal.InMemoryCommandJournal;
import exchange.marketdata.MarketDataEngine;
import exchange.model.ExchangeEvent;
import exchange.model.NewOrderCommand;
import exchange.model.OrderExecuted;
import exchange.model.OrderRejected;
import exchange.model.OrderType;
import exchange.model.RejectReason;
import exchange.model.SelfTradePreventionMode;
import exchange.model.Side;
import exchange.replication.InMemoryReplicationChannel;
import exchange.replication.PassiveReplica;
import exchange.replication.ReplicationGapException;
import exchange.replication.ReplicationRecord;
import exchange.risk.InMemoryRiskEngine;
import exchange.risk.RiskProfile;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ActivePassiveReplicationTest {

    @Test
    void replicatesGatewayRejectedCommandsAsSequencePreservingNonMatchingRecords() throws Exception {
        InMemoryReplicationChannel replication = new InMemoryReplicationChannel(128);
        try (PrimaryHarness primary = primary(replication)) {
            primary.riskEngine().setAvailableCash("BUYER1", 1_000_000L);
            primary.riskEngine().setAvailableCash("BUYER2", 100L);

            List<ExchangeEvent> accepted = primary.gateway().submit(limit("B1", "BUYER1", 10, "$10.00"));
            List<ExchangeEvent> rejected = primary.gateway().submit(limit("B2", "BUYER2", 10, "$10.00"));

            assertFalse(accepted.stream().anyMatch(OrderRejected.class::isInstance));
            OrderRejected rejection = assertInstanceOf(OrderRejected.class, rejected.get(0));
            assertEquals(RejectReason.RISK_BUYING_POWER, rejection.reason());

            List<ReplicationRecord> records = replication.replay();
            assertEquals(2, records.size());
            assertEquals(1L, records.get(0).sequenceNumber());
            assertTrue(records.get(0).acceptedForMatching());
            assertEquals(2L, records.get(1).sequenceNumber());
            assertFalse(records.get(1).acceptedForMatching());
            assertEquals(2L, replication.replicatedThrough());
        }
    }

    @Test
    void passiveReplicaReplaysAcceptedMatchingStreamAndAdvancesAcrossRejectedGaps() throws Exception {
        InMemoryReplicationChannel replication = new InMemoryReplicationChannel(128);
        try (PrimaryHarness primary = primary(replication)) {
            primary.riskEngine().setAvailableCash("BUYER1", 1_000_000L);
            primary.riskEngine().setAvailableCash("BUYER2", 100L);
            primary.riskEngine().setPosition("SELLER1", "AAPL", 10);

            primary.gateway().submit(limit("B1", "BUYER1", 10, "$10.00"));
            primary.gateway().submit(limit("B2", "BUYER2", 10, "$10.00"));
            primary.gateway().submit(new NewOrderCommand(0, "S1", "SELLER1", "AAPL", Side.SELL,
                    OrderType.MARKET, null, 10, SelfTradePreventionMode.CANCEL_NEWEST));
        }

        Sequencer standbySequencer = new Sequencer();
        InMemoryRiskEngine standbyRisk = new InMemoryRiskEngine(new RiskProfile(10_000_000_000L, 10_000_000,
                10_000_000_000L, false));
        MarketDataEngine standbyMarketData = new MarketDataEngine();
        PassiveReplica standby = new PassiveReplica(
                Set.of("AAPL"),
                new DeterministicMatchingEngine(Set.of("AAPL")),
                standbyRisk,
                standbyMarketData,
                standbySequencer);

        List<ExchangeEvent> replayedEvents = standby.applyAll(replication.replay());

        List<OrderExecuted> executions = replayedEvents.stream()
                .filter(OrderExecuted.class::isInstance)
                .map(OrderExecuted.class::cast)
                .toList();
        assertEquals(2, executions.size());
        assertTrue(executions.stream().allMatch(event -> event.sequenceNumber() == 3L));
        assertEquals(3L, standby.lastAppliedSequence());
        assertEquals(4L, standbySequencer.nextSequenceNumber());
    }

    @Test
    void passiveReplicaRejectsNonContiguousReplicationStream() throws Exception {
        InMemoryReplicationChannel replication = new InMemoryReplicationChannel(2);
        try (PrimaryHarness primary = primary(replication)) {
            primary.riskEngine().setAvailableCash("BUYER1", 1_000_000L);
            primary.gateway().submit(limit("B1", "BUYER1", 1, "$10.00"));
            primary.gateway().submit(limit("B2", "BUYER1", 1, "$10.01"));
            primary.gateway().submit(limit("B3", "BUYER1", 1, "$10.02"));
        }

        PassiveReplica standby = new PassiveReplica(
                Set.of("AAPL"),
                new DeterministicMatchingEngine(Set.of("AAPL")),
                new InMemoryRiskEngine(new RiskProfile(10_000_000_000L, 10_000_000,
                        10_000_000_000L, false)),
                new MarketDataEngine(),
                new Sequencer());

        List<ReplicationRecord> retainedOnly = replication.replay();
        assertEquals(2, retainedOnly.size());
        assertEquals(2L, retainedOnly.get(0).sequenceNumber());
        assertThrows(ReplicationGapException.class, () -> standby.applyAll(retainedOnly));
        assertEquals(0L, standby.lastAppliedSequence());
    }

    private static NewOrderCommand limit(String orderId, String clientId, int quantity, String price) throws Exception {
        return new NewOrderCommand(0, orderId, clientId, "AAPL", Side.BUY, OrderType.LIMIT,
                PriceFactory.makePrice(price), quantity, SelfTradePreventionMode.CANCEL_NEWEST);
    }

    private static PrimaryHarness primary(InMemoryReplicationChannel replication) {
        InMemoryCommandJournal journal = new InMemoryCommandJournal(4_096);
        InMemoryEventDispatcher dispatcher = new InMemoryEventDispatcher();
        InMemoryRiskEngine riskEngine = new InMemoryRiskEngine(new RiskProfile(10_000_000_000L, 10_000_000,
                10_000_000_000L, false));
        OrderGateway gateway = new OrderGateway(
                new GatewayValidator(Set.of("AAPL"), 1, 1, 1_000_000),
                riskEngine,
                new Sequencer(),
                journal,
                replication,
                new DeterministicMatchingEngine(Set.of("AAPL")),
                dispatcher);
        return new PrimaryHarness(gateway, journal, dispatcher, riskEngine);
    }

    private record PrimaryHarness(
            OrderGateway gateway,
            InMemoryCommandJournal journal,
            InMemoryEventDispatcher dispatcher,
            InMemoryRiskEngine riskEngine) implements AutoCloseable {
        @Override
        public void close() {
            gateway.close();
            dispatcher.close();
            journal.close();
        }
    }
}
