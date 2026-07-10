package persistence;

import logging.AuditEvent;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

public final class JdbcAuditRepository implements AuditRepository {
    private static final String POSTGRES_INSERT_SQL = """
            INSERT INTO audit_log
            (event_id, event_type, timestamp, user_id, product, data, hash, prev_hash)
            VALUES (?, ?, ?, ?, ?, ?::jsonb, ?, ?)
            """;
    private static final String PORTABLE_INSERT_SQL = """
            INSERT INTO audit_log
            (event_id, event_type, timestamp, user_id, product, data, hash, prev_hash)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """;

    private final DatabaseManager dbManager;

    public JdbcAuditRepository(DatabaseManager dbManager) {
        this.dbManager = dbManager;
    }

    @Override
    public void save(AuditEvent event) throws SQLException {
        try (Connection conn = dbManager.getConnection();
                PreparedStatement stmt = conn.prepareStatement(insertSql(conn))) {

            stmt.setString(1, event.getEventId());
            stmt.setString(2, event.getEventType().name());
            stmt.setTimestamp(3, Timestamp.from(event.getTimestamp()));
            stmt.setString(4, event.getUserId());
            stmt.setString(5, event.getProduct());
            stmt.setString(6, event.toJson());
            stmt.setString(7, event.getHash());
            stmt.setString(8, event.getPreviousHash());

            stmt.executeUpdate();
        }
    }

    private static String insertSql(Connection conn) throws SQLException {
        String database = conn.getMetaData().getDatabaseProductName();
        return database != null && database.toLowerCase().contains("postgres")
                ? POSTGRES_INSERT_SQL
                : PORTABLE_INSERT_SQL;
    }
}
