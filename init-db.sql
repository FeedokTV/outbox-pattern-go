CREATE TABLE IF NOT EXISTS transactions (
    id UUID PRIMARY KEY NOT NULL,
    sender VARCHAR(100) NOT NULL,
    recipient VARCHAR(100) NOT NULL,
    amount BIGINT NOT NULL, -- one of the best practices :) never use float
    sended_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE PUBLICATION transactions_cdc;