package persistence;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Manages failover between primary and secondary trading engine instances.
 * Monitors health, triggers failover, and handles failback.
 */
public class FailoverManager {
    private static final Logger logger = LoggerFactory.getLogger(FailoverManager.class);
    private static final FailoverManager instance = new FailoverManager();
    
    public enum EngineMode {
        PRIMARY,
        SECONDARY,
        UNAVAILABLE
    }
    
    private volatile EngineMode activeMode = EngineMode.PRIMARY;
    private final AtomicBoolean primaryHealthy = new AtomicBoolean(true);
    private final AtomicBoolean secondaryHealthy = new AtomicBoolean(true);
    private final AtomicLong lastFailoverTime = new AtomicLong(0);
    private final AtomicLong failoverCount = new AtomicLong(0);
    
    private static final long FAILOVER_COOLDOWN_MS = 30000; // 30 seconds
    private static final long HEALTH_CHECK_TIMEOUT_MS = 5000; // 5 seconds
    
    private FailoverManager() {
    }
    
    public static FailoverManager getInstance() {
        return instance;
    }
    
    /**
     * Update primary engine health status.
     */
    public void setPrimaryHealthy(boolean healthy) {
        boolean wasHealthy = primaryHealthy.getAndSet(healthy);
        
        if (!healthy && wasHealthy) {
            logger.error("PRIMARY engine reported UNHEALTHY");
            if (canFailover()) {
                executeFailover();
            }
        } else if (healthy && !wasHealthy && activeMode == EngineMode.SECONDARY) {
            logger.info("PRIMARY engine recovered, initiating failback");
            executeFailback();
        }
    }
    
    /**
     * Update secondary engine health status.
     */
    public void setSecondaryHealthy(boolean healthy) {
        secondaryHealthy.set(healthy);
        if (healthy) {
            logger.info("SECONDARY engine reported HEALTHY");
        } else {
            logger.warn("SECONDARY engine reported UNHEALTHY");
        }
    }
    
    /**
     * Check if failover is possible (respects cooldown period).
     */
    private boolean canFailover() {
        long timeSinceLastFailover = System.currentTimeMillis() - lastFailoverTime.get();
        if (timeSinceLastFailover < FAILOVER_COOLDOWN_MS) {
            logger.warn("Failover cooldown period active, {} ms remaining",
                FAILOVER_COOLDOWN_MS - timeSinceLastFailover);
            return false;
        }
        return secondaryHealthy.get();
    }
    
    /**
     * Execute failover to secondary engine.
     */
    private synchronized void executeFailover() {
        if (activeMode == EngineMode.SECONDARY) {
            logger.warn("Already in SECONDARY mode, failover not needed");
            return;
        }
        
        logger.error("========== INITIATING FAILOVER ==========");
        long startTime = System.currentTimeMillis();
        
        try {
            // 1. Stop accepting new orders on primary
            logger.info("Step 1: Pausing primary engine");
            // Implementation would call primary.pause()
            
            // 2. Load latest state snapshot
            logger.info("Step 2: Loading latest state snapshot from Kafka");
            EngineStateSnapshot snapshot = KafkaStateReplicator.getInstance().loadLatestSnapshotFromKafka();
            if (snapshot == null) {
                logger.error("Failed to load state snapshot, aborting failover");
                return;
            }
            logger.info("  Loaded snapshot: {}", snapshot);
            
            // 3. Restore secondary engine state
            logger.info("Step 3: Restoring secondary engine state");
            // Implementation would call secondary.restoreFromSnapshot(snapshot)
            
            // 4. Switch active mode
            activeMode = EngineMode.SECONDARY;
            lastFailoverTime.set(System.currentTimeMillis());
            failoverCount.incrementAndGet();
            
            // 5. Resume secondary
            logger.info("Step 4: Resuming secondary engine");
            // Implementation would call secondary.resume()
            
            long duration = System.currentTimeMillis() - startTime;
            logger.error("========== FAILOVER COMPLETE ({}ms) ==========", duration);
            
            // 6. Start failback monitor
            startFailbackMonitor();
            
        } catch (Exception e) {
            logger.error("Failover execution failed", e);
            activeMode = EngineMode.UNAVAILABLE;
        }
    }
    
    /**
     * Execute failback to primary engine.
     */
    private synchronized void executeFailback() {
        if (activeMode == EngineMode.PRIMARY) {
            logger.warn("Already in PRIMARY mode, failback not needed");
            return;
        }
        
        if (!primaryHealthy.get()) {
            logger.warn("PRIMARY engine not healthy, cannot failback");
            return;
        }
        
        logger.info("========== INITIATING FAILBACK ==========");
        long startTime = System.currentTimeMillis();
        
        try {
            // 1. Pause secondary
            logger.info("Step 1: Pausing secondary engine");
            // Implementation would call secondary.pause()
            
            // 2. Sync secondary state to primary
            logger.info("Step 2: Syncing secondary state to primary");
            EngineStateSnapshot currentState = KafkaStateReplicator.getInstance().getLatestSnapshot();
            // Implementation would call primary.restoreFromSnapshot(currentState)
            
            // 3. Resume primary
            logger.info("Step 3: Resuming primary engine");
            // Implementation would call primary.resume()
            
            // 4. Switch active mode
            activeMode = EngineMode.PRIMARY;
            
            long duration = System.currentTimeMillis() - startTime;
            logger.info("========== FAILBACK COMPLETE ({}ms) ==========", duration);
            
        } catch (Exception e) {
            logger.error("Failback execution failed", e);
        }
    }
    
    /**
     * Start background monitor for failback opportunity.
     */
    private void startFailbackMonitor() {
        new Thread(() -> {
            try {
                // Wait for primary to recover
                for (int i = 0; i < 30; i++) {
                    Thread.sleep(1000);
                    
                    // Check primary health (would call primary.healthCheck())
                    // if (primary.healthCheck()) {
                    //     executeFailback();
                    //     return;
                    // }
                }
                logger.warn("Primary did not recover within 30 seconds");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }).start();
    }
    
    /**
     * Get current engine mode.
     */
    public EngineMode getActiveMode() {
        return activeMode;
    }
    
    /**
     * Get failover statistics.
     */
    public FailoverStats getStats() {
        return new FailoverStats(
            activeMode,
            primaryHealthy.get(),
            secondaryHealthy.get(),
            failoverCount.get(),
            lastFailoverTime.get()
        );
    }
    
    /**
     * Failover statistics holder.
     */
    public static class FailoverStats {
        public final EngineMode activeMode;
        public final boolean primaryHealthy;
        public final boolean secondaryHealthy;
        public final long failoverCount;
        public final long lastFailoverTime;
        
        public FailoverStats(EngineMode mode, boolean primary, boolean secondary,
                            long count, long lastTime) {
            this.activeMode = mode;
            this.primaryHealthy = primary;
            this.secondaryHealthy = secondary;
            this.failoverCount = count;
            this.lastFailoverTime = lastTime;
        }
        
        @Override
        public String toString() {
            return String.format(
                "FailoverStats{mode=%s, primary=%s, secondary=%s, failovers=%d}",
                activeMode, primaryHealthy, secondaryHealthy, failoverCount);
        }
    }
}
