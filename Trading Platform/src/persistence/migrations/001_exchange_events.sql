-- Durable ECN event-sourcing journal.
-- One row is persisted per sequenced command. The payload JSONB contains the
-- immutable event batch emitted for that sequence_id.

CREATE TABLE IF NOT EXISTS exchange_events (
    sequence_id BIGINT PRIMARY KEY,
    inbound_timestamp BIGINT NOT NULL,
    event_type VARCHAR(64) NOT NULL,
    symbol VARCHAR(32) NOT NULL,
    payload JSONB NOT NULL,
    persisted_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_exchange_events_symbol_sequence
    ON exchange_events (symbol, sequence_id);

CREATE INDEX IF NOT EXISTS idx_exchange_events_inbound_timestamp
    ON exchange_events (inbound_timestamp);

CREATE INDEX IF NOT EXISTS idx_exchange_events_payload_gin
    ON exchange_events USING GIN (payload);

COMMENT ON TABLE exchange_events IS 'Append-only sequenced ECN event journal for deterministic replay';
COMMENT ON COLUMN exchange_events.sequence_id IS 'Monotonic command sequence id; primary replay ordering key';
COMMENT ON COLUMN exchange_events.inbound_timestamp IS 'Gateway-assigned inbound timestamp in epoch nanoseconds';
COMMENT ON COLUMN exchange_events.event_type IS 'Single event type, or EXCHANGE_EVENT_BATCH when a command emitted multiple events';
COMMENT ON COLUMN exchange_events.symbol IS 'Symbol shard that produced the event batch';
COMMENT ON COLUMN exchange_events.payload IS 'Immutable JSONB event envelope for the sequenced command';
