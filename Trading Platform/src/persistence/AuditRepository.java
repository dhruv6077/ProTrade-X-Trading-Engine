package persistence;

import logging.AuditEvent;

import java.sql.SQLException;

public interface AuditRepository {
    void save(AuditEvent event) throws SQLException;
}
