-- Grant permissions
GRANT ALL PRIVILEGES ON DATABASE targetdb TO kafka_user;

-- Create target schema
CREATE SCHEMA IF NOT EXISTS pipeline;

-- Target tables (mirror source structure + CDC metadata columns)
CREATE TABLE IF NOT EXISTS pipeline.orders (
    id           BIGINT PRIMARY KEY,
    customer_id  BIGINT,
    product_id   BIGINT,
    quantity     INT,
    amount       NUMERIC(10,2),
    status       VARCHAR(50),
    created_at   TIMESTAMP,
    updated_at   TIMESTAMP,
    _cdc_op      VARCHAR(10),
    _ingested_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS pipeline.customers (
    id           BIGINT PRIMARY KEY,
    name         VARCHAR(255),
    email        VARCHAR(255),
    phone        VARCHAR(50),
    created_at   TIMESTAMP,
    _cdc_op      VARCHAR(10),
    _ingested_at TIMESTAMP DEFAULT NOW()
);

