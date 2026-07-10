package persistence;

import logging.AuditEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

/**
 * Database-backed audit logger that persists audit events to PostgreSQL.
 * Works in conjunction with file-based audit logging for redundancy.
 */
public class DatabaseAuditLogger implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(DatabaseAuditLogger.class);
    private static final int DEFAULT_QUEUE_CAPACITY = 65_536;

    private static DatabaseAuditLogger instance;
    private static volatile boolean dbWarningLogged = false;

    private final DatabaseManager dbManager;
    private final AuditRepository auditRepository;
    private final BooleanSupplier databaseAvailable;
    private final ArrayBlockingQueue<AuditEvent> asyncQueue;
    private final AtomicBoolean running = new AtomicBoolean();
    private final AtomicLong droppedEvents = new AtomicLong();
    private final Thread worker;

    private DatabaseAuditLogger() {
        this(DatabaseManager.getInstance());
    }

    DatabaseAuditLogger(DatabaseManager dbManager) {
        this(dbManager, new JdbcAuditRepository(dbManager), true, DEFAULT_QUEUE_CAPACITY);
    }

    public DatabaseAuditLogger(DatabaseManager dbManager, AuditRepository auditRepository) {
        this(dbManager, auditRepository, false, 0);
    }

    public DatabaseAuditLogger(BooleanSupplier databaseAvailable, AuditRepository auditRepository) {
        this(null, databaseAvailable, auditRepository, false, 0);
    }

    private DatabaseAuditLogger(
            DatabaseManager dbManager,
            AuditRepository auditRepository,
            boolean async,
            int queueCapacity) {
        this(dbManager, dbManager::isInitialized, auditRepository, async, queueCapacity);
    }

    private DatabaseAuditLogger(
            DatabaseManager dbManager,
            BooleanSupplier databaseAvailable,
            AuditRepository auditRepository,
            boolean async,
            int queueCapacity) {
        this.dbManager = dbManager;
        this.auditRepository = Objects.requireNonNull(auditRepository, "auditRepository");
        this.databaseAvailable = Objects.requireNonNull(databaseAvailable, "databaseAvailable");
        this.asyncQueue = async ? new ArrayBlockingQueue<>(queueCapacity) : null;
        this.worker = async ? new Thread(this::drainLoop, "database-audit-writer") : null;
        if (worker != null) {
            worker.setDaemon(true);
            running.set(true);
            worker.start();
        }
    }

    /**
     * Gets the singleton instance of DatabaseAuditLogger.
     */
    public static synchronized DatabaseAuditLogger getInstance() {
        if (instance == null) {
            instance = new DatabaseAuditLogger();
        }
        return instance;
    }

    public void logEvent(AuditEvent event) {
        Objects.requireNonNull(event, "event");
        if (!databaseAvailable.getAsBoolean()) {
            if (!dbWarningLogged) {
                logger.warn("Database not initialized, skipping database audit log (suppressing future warnings)");
                dbWarningLogged = true;
            }
            return;
        }

        if (asyncQueue != null) {
            if (!asyncQueue.offer(event)) {
                long dropped = droppedEvents.incrementAndGet();
                if (dropped == 1 || dropped % 10_000 == 0) {
                    logger.error("Database audit queue full; dropped {} audit event(s)", dropped);
                }
            }
            return;
        }

        persist(event);
    }

    private void drainLoop() {
        while (running.get() || !asyncQueue.isEmpty()) {
            try {
                AuditEvent event = asyncQueue.poll(250, TimeUnit.MILLISECONDS);
                if (event != null && databaseAvailable.getAsBoolean()) {
                    persist(event);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        AuditEvent event;
        while ((event = asyncQueue.poll()) != null) {
            if (databaseAvailable.getAsBoolean()) {
                persist(event);
            }
        }
    }

    private void persist(AuditEvent event) {
        try {
            auditRepository.save(event);
            logger.debug("Audit event persisted to database: {}", event.getEventId());
        } catch (SQLException e) {
            logger.error("Failed to persist audit event to database: {}", event.getEventId(), e);
            // Don't throw exception - we don't want to fail the business operation
            // File-based logging will still work
        }
    }

    /**
     * Retrieves audit events for a specific user.
     */
    public List<AuditEvent> getEventsForUser(String userId, int limit) {
        List<AuditEvent> events = new ArrayList<>();

        String sql = """
                SELECT event_id, event_type, timestamp, user_id, product, data, hash, prev_hash
                FROM audit_log
                WHERE user_id = ?
                ORDER BY timestamp DESC
                LIMIT ?
                """;

        try (Connection conn = dbManager.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, userId);
            stmt.setInt(2, limit);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    // Note: This is a simplified version
                    // In production, you'd reconstruct the full AuditEvent
                    logger.debug("Retrieved event: {}", rs.getString("event_id"));
                }
            }

        } catch (SQLException e) {
            logger.error("Failed to retrieve audit events for user: {}", userId, e);
        }

        return events;
    }

    /**
     * Retrieves audit events for a specific product.
     */
    public List<AuditEvent> getEventsForProduct(String product, int limit) {
        List<AuditEvent> events = new ArrayList<>();

        String sql = """
                SELECT event_id, event_type, timestamp, user_id, product, data, hash, prev_hash
                FROM audit_log
                WHERE product = ?
                ORDER BY timestamp DESC
                LIMIT ?
                """;

        try (Connection conn = dbManager.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, product);
            stmt.setInt(2, limit);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    logger.debug("Retrieved event: {}", rs.getString("event_id"));
                }
            }

        } catch (SQLException e) {
            logger.error("Failed to retrieve audit events for product: {}", product, e);
        }

        return events;
    }

    /**
     * Verifies the hash chain integrity in the database.
     */
    public boolean verifyHashChainIntegrity() {
        String sql = "SELECT is_valid, error_message FROM verify_hash_chain()";

        try (Connection conn = dbManager.getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {

            if (rs.next()) {
                boolean isValid = rs.getBoolean(1);
                String message = rs.getString(2);

                if (isValid) {
                    logger.info("Database hash chain verification: {}", message);
                } else {
                    logger.error("Database hash chain verification FAILED: {}", message);
                }

                return isValid;
            }

        } catch (SQLException e) {
            logger.error("Failed to verify hash chain integrity", e);
        }

        return false;
    }

    /**
     * Gets the total count of audit events.
     */
    public long getAuditEventCount() {
        String sql = "SELECT COUNT(*) FROM audit_log";

        try (Connection conn = dbManager.getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {

            if (rs.next()) {
                return rs.getLong(1);
            }

        } catch (SQLException e) {
            logger.error("Failed to get audit event count", e);
        }

        return 0;
    }

    public long droppedEvents() {
        return droppedEvents.get();
    }

    @Override
    public void close() {
        if (worker == null) {
            return;
        }
        running.set(false);
        worker.interrupt();
        try {
            worker.join(5_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
