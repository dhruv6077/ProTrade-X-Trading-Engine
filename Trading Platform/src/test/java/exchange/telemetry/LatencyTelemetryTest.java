package exchange.telemetry;

import exchange.model.NewOrderCommand;
import exchange.model.OrderType;
import exchange.model.SelfTradePreventionMode;
import exchange.model.Side;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import Price.PriceFactory;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LatencyTelemetryTest {
    private final LatencyTelemetry telemetry = LatencyTelemetry.getInstance();

    @AfterEach
    void resetTelemetry() {
        telemetry.reset();
    }

    @Test
    void recordsTailLatencyWithoutAllocatingPerMeasurementObjects() {
        telemetry.reset();

        telemetry.recordEngineToDispatch(1_000L, 1_750L);
        telemetry.recordTickToTrade(1_000L, 2_250L);
        telemetry.recordEgressWrite(2_000L, 2_500L);

        LatencyTelemetry.LatencyReport report = telemetry.snapshot();

        assertEquals(1, report.engineToDispatch().count());
        assertTrue(report.engineToDispatch().p99Ns() >= 750L);
        assertEquals(1, report.tickToTrade().count());
        assertTrue(report.tickToTrade().p99Ns() >= 1_250L);
        assertEquals(1, report.egressWrite().count());
        assertTrue(report.egressWrite().p99Ns() >= 500L);
    }

    @Test
    void ignoresInvalidClockDeltas() {
        telemetry.reset();

        telemetry.recordTickToTrade(0L, 100L);
        telemetry.recordTickToTrade(200L, 100L);

        assertEquals(0, telemetry.snapshot().tickToTrade().count());
    }

    @Test
    void newOrderCommandPreservesIngressTimestampAcrossSequencing() throws Exception {
        long ingressTimeNs = 987_654_321L;
        NewOrderCommand raw = new NewOrderCommand(
                0L,
                Instant.EPOCH,
                "B1",
                "CLIENT1",
                "AAPL",
                Side.BUY,
                OrderType.LIMIT,
                PriceFactory.makePrice("$10.00"),
                10,
                SelfTradePreventionMode.CANCEL_NEWEST,
                ingressTimeNs);

        NewOrderCommand sequenced = raw.withSequencing(42L, Instant.parse("2026-06-30T00:00:00Z"));

        assertEquals(42L, sequenced.sequenceNumber());
        assertEquals(ingressTimeNs, sequenced.ingressTimeNs());
    }
}
