-- Grant CDC permissions to kafka_user (used by Debezium)
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'kafka_user'@'%';
FLUSH PRIVILEGES;

-- Sample source tables
CREATE TABLE IF NOT EXISTS orders (
    id          BIGINT AUTO_INCREMENT PRIMARY KEY,
    customer_id BIGINT NOT NULL,
    product_id  BIGINT NOT NULL,
    quantity    INT NOT NULL,
    amount      DECIMAL(10,2) NOT NULL,
    status      VARCHAR(50) DEFAULT 'PENDING',
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS customers (
    id         BIGINT AUTO_INCREMENT PRIMARY KEY,
    name       VARCHAR(255) NOT NULL,
    email      VARCHAR(255) UNIQUE NOT NULL,
    phone      VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert sample data
INSERT INTO customers (name, email, phone) VALUES
  ('Alice Johnson', 'alice@example.com', '+1-555-0101'),
  ('Bob Smith', 'bob@example.com', '+1-555-0102');

INSERT INTO orders (customer_id, product_id, quantity, amount, status) VALUES
  (1, 101, 2, 49.99, 'COMPLETED'),
  (2, 102, 1, 99.00, 'PENDING');