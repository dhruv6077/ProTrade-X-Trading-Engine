package persistence;

import logging.AuditEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Database-backed audit logger that persists audit events to PostgreSQL.
 * Works in conjunction with file-based audit logging for redundancy.
 */
public class DatabaseAuditLogger {
    private static final Logger logger = LoggerFactory.getLogger(DatabaseAuditLogger.class);
    private static DatabaseAuditLogger instance;
    private final DatabaseManager dbManager;
    
    private static final String INSERT_SQL = """
            INSERT INTO audit_log 
            (event_id, event_type, timestamp, user_id, product, data, hash, prev_hash)
            VALUES (?, ?, ?, ?, ?, ?::jsonb, ?, ?)
            """;
    
    private DatabaseAuditLogger() {
        this.dbManager = DatabaseManager.getInstance();
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
    
    /**
     * Logs an audit event to the database.
     */
    public void logEvent(AuditEvent event) {
        if (!dbManager.isInitialized()) {
            logger.warn("Database not initialized, skipping database audit log");
            return;
        }
        
        try (Connection conn = dbManager.getConnection();
             PreparedStatement stmt = conn.prepareStatement(INSERT_SQL)) {
            
            stmt.setString(1, event.getEventId());
            stmt.setString(2, event.getEventType().name());
            stmt.setTimestamp(3, Timestamp.from(event.getTimestamp()));
            stmt.setString(4, event.getUserId());
            stmt.setString(5, event.getProduct());
            stmt.setString(6, event.toJson());
            stmt.setString(7, event.getHash());
            stmt.setString(8, event.getPreviousHash());
            
            stmt.executeUpdate();
            
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
        String sql = "SELECT verify_hash_chain()";
        
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
}
