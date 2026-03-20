-- Grant schema permissions to kafka_user
GRANT ALL PRIVILEGES ON DATABASE targetdb TO kafka_user;

-- Create target schema — Debezium will create tables automatically inside this schema
CREATE SCHEMA IF NOT EXISTS pipeline;
GRANT ALL PRIVILEGES ON SCHEMA pipeline TO kafka_user;

