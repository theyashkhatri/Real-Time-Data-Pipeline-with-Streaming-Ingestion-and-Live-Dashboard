-- Analytics schema for the real-time order pipeline.
-- Runs automatically on first start of the Postgres container.

CREATE TABLE IF NOT EXISTS orders (
    order_id       TEXT PRIMARY KEY,               -- natural key => idempotent inserts
    user_id        INTEGER        NOT NULL,
    product        TEXT           NOT NULL,
    category       TEXT           NOT NULL,
    quantity       INTEGER        NOT NULL CHECK (quantity > 0),
    unit_price     NUMERIC(10, 2) NOT NULL CHECK (unit_price > 0),
    total_amount   NUMERIC(12, 2) NOT NULL,
    country        CHAR(2)        NOT NULL,
    payment_method TEXT           NOT NULL,
    status         TEXT           NOT NULL,
    event_time     TIMESTAMPTZ    NOT NULL,        -- when the order happened
    ingested_at    TIMESTAMPTZ    NOT NULL DEFAULT now()  -- when the pipeline stored it
);

CREATE INDEX IF NOT EXISTS idx_orders_event_time ON orders (event_time DESC);
CREATE INDEX IF NOT EXISTS idx_orders_category   ON orders (category);

-- Messages that failed validation are kept for inspection instead of being dropped.
CREATE TABLE IF NOT EXISTS dead_letter (
    id              BIGSERIAL PRIMARY KEY,
    kafka_topic     TEXT        NOT NULL,
    kafka_partition INTEGER     NOT NULL,
    kafka_offset    BIGINT      NOT NULL,
    raw_payload     TEXT,
    error_reason    TEXT        NOT NULL,
    received_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (kafka_topic, kafka_partition, kafka_offset)  -- safe to replay
);

CREATE INDEX IF NOT EXISTS idx_dead_letter_received_at ON dead_letter (received_at DESC);

-- Per-minute rollup used by ad-hoc analysis (the dashboard queries the base tables directly).
CREATE OR REPLACE VIEW orders_per_minute AS
SELECT
    date_trunc('minute', event_time)                          AS minute,
    count(*)                                                  AS orders,
    sum(total_amount) FILTER (WHERE status <> 'cancelled')    AS revenue,
    avg(total_amount)                                         AS avg_order_value
FROM orders
GROUP BY 1
ORDER BY 1 DESC;
