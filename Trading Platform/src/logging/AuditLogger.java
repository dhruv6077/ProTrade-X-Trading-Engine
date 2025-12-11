package logging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import persistence.DatabaseAuditLogger;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Main audit logger with hash chain implementation for non-repudiation.
 * Ensures tamper-proof logging by linking each log entry to the previous one.
 */
public class AuditLogger {
    private static final Logger auditLog = LoggerFactory.getLogger("AUDIT");
    private static final Logger logger = LoggerFactory.getLogger(AuditLogger.class);
    
    private static AuditLogger instance;
    private String previousHash = "0";
    private final MessageDigest digest;
    private final Object lock = new Object();
    
    private AuditLogger() {
        try {
            this.digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 algorithm not available", e);
        }
    }
    
    /**
     * Gets the singleton instance of AuditLogger.
     */
    public static synchronized AuditLogger getInstance() {
        if (instance == null) {
            instance = new AuditLogger();
        }
        return instance;
    }
    
    /**
     * Logs an audit event with hash chain protection.
     */
    public void logEvent(AuditEvent event) {
        synchronized (lock) {
            // Set previous hash
            event.setPreviousHash(previousHash);
            
            // Calculate hash for this event
            String dataToHash = previousHash + event.getDataForHashing();
            String currentHash = calculateHash(dataToHash);
            event.setHash(currentHash);
            
            // Log the event to file
            auditLog.info(event.toJson());
            
            // Also log to database
            try {
                DatabaseAuditLogger.getInstance().logEvent(event);
            } catch (Exception e) {
                logger.warn("Failed to log event to database", e);
            }
            
            // Update previous hash for next event
            previousHash = currentHash;
            
            logger.debug("Audit event logged: {} - {}", event.getEventType(), event.getEventId());
        }
    }
    
    /**
     * Convenience method to log an event with minimal information.
     */
    public void logEvent(AuditEventType eventType, String userId, String product) {
        AuditEvent event = new AuditEvent.Builder()
                .eventType(eventType)
                .userId(userId)
                .product(product)
                .build();
        logEvent(event);
    }
    
    /**
     * Calculates SHA-256 hash of the input string.
     */
    private String calculateHash(String input) {
        byte[] hashBytes = digest.digest(input.getBytes(StandardCharsets.UTF_8));
        return bytesToHex(hashBytes);
    }
    
    /**
     * Converts byte array to hexadecimal string.
     */
    private String bytesToHex(byte[] bytes) {
        StringBuilder hexString = new StringBuilder(2 * bytes.length);
        for (byte b : bytes) {
            String hex = Integer.toHexString(0xff & b);
            if (hex.length() == 1) {
                hexString.append('0');
            }
            hexString.append(hex);
        }
        return hexString.toString();
    }
    
    /**
     * Gets the current hash (for testing/validation).
     */
    public String getCurrentHash() {
        synchronized (lock) {
            return previousHash;
        }
    }
    
    /**
     * Resets the hash chain (should only be used for testing).
     */
    public void resetHashChain() {
        synchronized (lock) {
            previousHash = "0";
            logger.warn("Hash chain has been reset");
        }
    }
}
