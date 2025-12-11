package sim;

import logging.AuditEvent;
import persistence.DatabaseManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.LocalDate;
import java.util.*;

/**
 * Loads historical trading data from PostgreSQL for backtesting.
 * Retrieves audit events within specified date range and products.
 */
public class HistoricalDataLoader {
    private static final Logger logger = LoggerFactory.getLogger(HistoricalDataLoader.class);
    private final DatabaseManager dbManager;
    
    public HistoricalDataLoader() {
        this.dbManager = DatabaseManager.getInstance();
    }
    
    /**
     * Load all audit events within date range for specified products.
     * 
     * @param startDate Start of backtest period
     * @param endDate End of backtest period
     * @param products List of product symbols to include
     * @return List of audit events sorted by timestamp
     */
    public List<AuditEvent> loadEvents(LocalDate startDate, LocalDate endDate, List<String> products) {
        if (!dbManager.isInitialized()) {
            logger.warn("Database not initialized, cannot load historical data");
            return new ArrayList<>();
        }
        
        List<AuditEvent> events = new ArrayList<>();
        
        String sql = """
            SELECT event_id, event_type, timestamp, user_id, product, data, hash, prev_hash
            FROM audit_log
            WHERE timestamp BETWEEN ? AND ?
            AND product = ANY(?)
            ORDER BY timestamp ASC
        """;
        
        try (Connection conn = dbManager.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setTimestamp(1, Timestamp.valueOf(startDate.atStartOfDay()));
            stmt.setTimestamp(2, Timestamp.valueOf(endDate.atTime(23, 59, 59)));
            
            // Create array of product symbols
            Array productsArray = conn.createArrayOf("varchar", products.toArray());
            stmt.setArray(3, productsArray);
            
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    AuditEvent event = AuditEvent.fromResultSet(rs);
                    events.add(event);
                }
            }
            
            logger.info("Loaded {} historical events from {} to {}", 
                events.size(), startDate, endDate);
            
        } catch (SQLException e) {
            logger.error("Failed to load historical data from database", e);
        }
        
        return events;
    }
    
    /**
     * Load events for a single product.
     */
    public List<AuditEvent> loadEventsForProduct(LocalDate startDate, LocalDate endDate, String product) {
        return loadEvents(startDate, endDate, Collections.singletonList(product));
    }
    
    /**
     * Get available date range in database.
     * 
     * @return Object with minDate and maxDate
     */
    public DateRange getAvailableDateRange() {
        String sql = """
            SELECT MIN(DATE(timestamp)) as min_date, MAX(DATE(timestamp)) as max_date
            FROM audit_log
        """;
        
        try (Connection conn = dbManager.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            
            if (rs.next()) {
                Date minDate = rs.getDate("min_date");
                Date maxDate = rs.getDate("max_date");
                return new DateRange(
                    minDate != null ? minDate.toLocalDate() : null,
                    maxDate != null ? maxDate.toLocalDate() : null
                );
            }
            
        } catch (SQLException e) {
            logger.error("Failed to get available date range", e);
        }
        
        return new DateRange(null, null);
    }
    
    /**
     * Get list of unique products in database.
     */
    public List<String> getAvailableProducts() {
        String sql = "SELECT DISTINCT product FROM audit_log ORDER BY product";
        List<String> products = new ArrayList<>();
        
        try (Connection conn = dbManager.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            
            while (rs.next()) {
                products.add(rs.getString("product"));
            }
            
        } catch (SQLException e) {
            logger.error("Failed to get available products", e);
        }
        
        return products;
    }
    
    /**
     * Get event count statistics.
     */
    public EventStats getEventStats(LocalDate startDate, LocalDate endDate) {
        String sql = """
            SELECT 
                COUNT(*) as total_events,
                COUNT(DISTINCT product) as product_count,
                COUNT(DISTINCT user_id) as user_count
            FROM audit_log
            WHERE timestamp BETWEEN ? AND ?
        """;
        
        try (Connection conn = dbManager.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setTimestamp(1, Timestamp.valueOf(startDate.atStartOfDay()));
            stmt.setTimestamp(2, Timestamp.valueOf(endDate.atTime(23, 59, 59)));
            
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return new EventStats(
                        rs.getLong("total_events"),
                        rs.getInt("product_count"),
                        rs.getInt("user_count")
                    );
                }
            }
            
        } catch (SQLException e) {
            logger.error("Failed to get event statistics", e);
        }
        
        return new EventStats(0, 0, 0);
    }
    
    public static class DateRange {
        public final LocalDate minDate;
        public final LocalDate maxDate;
        
        public DateRange(LocalDate minDate, LocalDate maxDate) {
            this.minDate = minDate;
            this.maxDate = maxDate;
        }
    }
    
    public static class EventStats {
        public final long totalEvents;
        public final int productCount;
        public final int userCount;
        
        public EventStats(long total, int products, int users) {
            this.totalEvents = total;
            this.productCount = products;
            this.userCount = users;
        }
    }
}
