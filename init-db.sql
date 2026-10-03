CREATE TABLE IF NOT EXISTS transactions (
    id UUID PRIMARY KEY NOT NULL,
    sender VARCHAR(100) NOT NULL,
    recipient VARCHAR(100) NOT NULL,
    amount BIGINT NOT NULL, -- one of the best practices :) never use float
    sended_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- For V1 --
CREATE TABLE IF NOT EXISTS outbox (
    id UUID PRIMARY KEY NOT NULL,
    aggregate_id VARCHAR(100) NOT NULL, -- Kafka key
    event_type VARCHAR(100) NOT NULL,
    payload JSONB NOT NULL, -- full event envelope
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    locked_until TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP, -- relay lease: row is free once this is in the past
    sent_at TIMESTAMPTZ -- NULL until the relay has published the event
);

-- Relay only looks for unsent rows, so index just those
CREATE INDEX IF NOT EXISTS outbox_unsent_idx ON outbox (id) WHERE sent_at IS NULL;

-- For V2 --
CREATE PUBLICATION transactions_cdc;