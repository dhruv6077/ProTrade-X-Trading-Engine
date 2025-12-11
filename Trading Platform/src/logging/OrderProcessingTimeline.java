package logging;

import java.util.*;

/**
 * Tracks the timeline of order processing with nanosecond precision.
 * Used to identify performance bottlenecks in the matching engine.
 * 
 * Critical timestamps:
 * T0: Order arrival at API
 * T1: Order deserialization complete
 * T2: Order validation complete
 * T3: Write lock acquired
 * T4: Matching begins
 * T5: Matching complete
 * T6: Trade execution begins
 * T7: Trade execution complete
 * T8: Audit log write begins
 * T9: Audit log write complete
 * T10: Response sent to client
 */
public class OrderProcessingTimeline {
    public long t0_arrival;           // Message arrival timestamp
    public long t1_deserialized;      // After JSON/protobuf parsing
    public long t2_validated;         // After business logic validation
    public long t3_lockAcquired;      // Write lock obtained
    public long t4_matchingBegins;    // Matching engine invoked
    public long t5_matchingComplete;  // All fills determined
    public long t6_executionBegins;   // Trade execution starts
    public long t7_executionDone;     // Trade execution completes
    public long t8_auditBegins;       // Audit log write starts
    public long t9_auditComplete;     // Audit log write done
    public long t10_responseSent;     // Response sent to client
    
    // Order identification for tracking
    public String orderId;
    public String symbol;
    
    public OrderProcessingTimeline(String orderId, String symbol) {
        this.orderId = orderId;
        this.symbol = symbol;
    }
    
    /**
     * Get end-to-end latency in nanoseconds.
     */
    public long getEndToEndLatency() {
        return t10_responseSent - t0_arrival;
    }
    
    /**
     * Get end-to-end latency in microseconds.
     */
    public double getEndToEndLatencyMicros() {
        return getEndToEndLatency() / 1000.0;
    }
    
    /**
     * Get latency for matching phase only.
     */
    public long getMatchingLatency() {
        return t5_matchingComplete - t4_matchingBegins;
    }
    
    /**
     * Get latency for auditing phase.
     */
    public long getAuditingLatency() {
        return t9_auditComplete - t8_auditBegins;
    }
    
    /**
     * Get latency for lock acquisition.
     */
    public long getLockAcquisitionLatency() {
        return t3_lockAcquired - t2_validated;
    }
    
    /**
     * Get breakdown of all phases.
     */
    public Map<String, Long> getPhaseBreakdown() {
        Map<String, Long> phases = new LinkedHashMap<>();
        phases.put("Deserialization", t1_deserialized - t0_arrival);
        phases.put("Validation", t2_validated - t1_deserialized);
        phases.put("Lock Acquisition", t3_lockAcquired - t2_validated);
        phases.put("Matching", t5_matchingComplete - t4_matchingBegins);
        phases.put("Execution", t7_executionDone - t6_executionBegins);
        phases.put("Auditing", t9_auditComplete - t8_auditBegins);
        phases.put("Response", t10_responseSent - t9_auditComplete);
        return phases;
    }
    
    /**
     * Log detailed breakdown for analysis.
     */
    public void logTimeline(org.slf4j.Logger logger) {
        logger.info("=== Order Processing Timeline: {} ({}) ===", orderId, symbol);
        logger.info("End-to-End Latency: {:.2f} µs", getEndToEndLatencyMicros());
        
        Map<String, Long> phases = getPhaseBreakdown();
        phases.forEach((phase, nanos) -> {
            logger.info("  {}: {:.2f} µs", phase, nanos / 1000.0);
        });
        
        logger.info("=== End Timeline ===");
    }
    
    /**
     * Check if this timeline exceeds latency thresholds.
     */
    public LatencyViolation checkThresholds() {
        if (getEndToEndLatency() > 1_000_000) {  // 1ms
            return new LatencyViolation("E2E", getEndToEndLatency(), 1_000_000);
        }
        if (getMatchingLatency() > 100_000) {    // 100µs
            return new LatencyViolation("Matching", getMatchingLatency(), 100_000);
        }
        if (getAuditingLatency() > 500_000) {    // 500µs
            return new LatencyViolation("Auditing", getAuditingLatency(), 500_000);
        }
        return null;
    }
    
    /**
     * Represents a latency threshold violation.
     */
    public static class LatencyViolation {
        public String phase;
        public long actualNanos;
        public long thresholdNanos;
        
        public LatencyViolation(String phase, long actual, long threshold) {
            this.phase = phase;
            this.actualNanos = actual;
            this.thresholdNanos = threshold;
        }
        
        public double getActualMicros() { return actualNanos / 1000.0; }
        public double getThresholdMicros() { return thresholdNanos / 1000.0; }
        
        @Override
        public String toString() {
            return String.format("%s phase exceeded: %.2f µs (threshold: %.2f µs)",
                phase, getActualMicros(), getThresholdMicros());
        }
    }
}
