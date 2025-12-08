-- Trading Platform Audit Log Database Schema
-- PostgreSQL 12+

-- Create database (run separately as superuser if needed)
-- CREATE DATABASE trading_platform;

-- Audit Log Table
CREATE TABLE IF NOT EXISTS audit_log (
    id BIGSERIAL PRIMARY KEY,
    event_id VARCHAR(36) NOT NULL UNIQUE,
    event_type VARCHAR(50) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    user_id VARCHAR(50),
    product VARCHAR(10),
    data JSONB NOT NULL,
    hash VARCHAR(64) NOT NULL,
    prev_hash VARCHAR(64) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for efficient querying
CREATE INDEX IF NOT EXISTS idx_audit_timestamp ON audit_log(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_audit_user ON audit_log(user_id);
CREATE INDEX IF NOT EXISTS idx_audit_product ON audit_log(product);
CREATE INDEX IF NOT EXISTS idx_audit_type ON audit_log(event_type);
CREATE INDEX IF NOT EXISTS idx_audit_event_id ON audit_log(event_id);
CREATE INDEX IF NOT EXISTS idx_audit_created_at ON audit_log(created_at DESC);

-- JSONB index for data field queries
CREATE INDEX IF NOT EXISTS idx_audit_data_gin ON audit_log USING GIN (data);

-- Comments for documentation
COMMENT ON TABLE audit_log IS 'Audit trail for all trading platform operations';
COMMENT ON COLUMN audit_log.event_id IS 'Unique identifier for the event (UUID)';
COMMENT ON COLUMN audit_log.event_type IS 'Type of audit event (ORDER_PLACED, TRADE_EXECUTED, etc.)';
COMMENT ON COLUMN audit_log.timestamp IS 'When the event occurred';
COMMENT ON COLUMN audit_log.user_id IS 'User who triggered the event';
COMMENT ON COLUMN audit_log.product IS 'Trading product/symbol involved';
COMMENT ON COLUMN audit_log.data IS 'Event-specific data in JSON format';
COMMENT ON COLUMN audit_log.hash IS 'SHA-256 hash of this event (for non-repudiation)';
COMMENT ON COLUMN audit_log.prev_hash IS 'Hash of previous event (hash chain)';
COMMENT ON COLUMN audit_log.created_at IS 'When the record was inserted into database';

-- View for recent audit events
CREATE OR REPLACE VIEW recent_audit_events AS
SELECT 
    event_id,
    event_type,
    timestamp,
    user_id,
    product,
    data,
    created_at
FROM audit_log
ORDER BY timestamp DESC
LIMIT 1000;

-- View for trade summary
CREATE OR REPLACE VIEW trade_summary AS
SELECT 
    product,
    COUNT(*) as trade_count,
    DATE(timestamp) as trade_date,
    data->>'price' as price,
    SUM((data->>'quantity')::int) as total_quantity
FROM audit_log
WHERE event_type = 'TRADE_EXECUTED'
GROUP BY product, DATE(timestamp), data->>'price'
ORDER BY trade_date DESC, product;

-- View for user activity
CREATE OR REPLACE VIEW user_activity_summary AS
SELECT 
    user_id,
    event_type,
    COUNT(*) as event_count,
    MAX(timestamp) as last_activity
FROM audit_log
WHERE user_id IS NOT NULL
GROUP BY user_id, event_type
ORDER BY user_id, event_count DESC;

-- Function to verify hash chain integrity
CREATE OR REPLACE FUNCTION verify_hash_chain()
RETURNS TABLE(is_valid BOOLEAN, error_message TEXT) AS $$
DECLARE
    prev_hash_val VARCHAR(64) := '0';
    current_record RECORD;
BEGIN
    FOR current_record IN 
        SELECT id, event_id, hash, prev_hash 
        FROM audit_log 
        ORDER BY id ASC
    LOOP
        IF current_record.prev_hash != prev_hash_val THEN
            RETURN QUERY SELECT FALSE, 
                'Hash chain broken at event_id: ' || current_record.event_id || 
                ' (expected prev_hash: ' || prev_hash_val || 
                ', found: ' || current_record.prev_hash || ')';
            RETURN;
        END IF;
        prev_hash_val := current_record.hash;
    END LOOP;
    
    RETURN QUERY SELECT TRUE, 'Hash chain is valid'::TEXT;
END;
$$ LANGUAGE plpgsql;

-- Grant permissions (adjust as needed for your security requirements)
-- GRANT SELECT, INSERT ON audit_log TO trading_app_user;
-- GRANT USAGE, SELECT ON SEQUENCE audit_log_id_seq TO trading_app_user;
-- GRANT SELECT ON recent_audit_events, trade_summary, user_activity_summary TO trading_app_user;
