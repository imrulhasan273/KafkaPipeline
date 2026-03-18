# Environment Setup

## Directory Structure

```
kafka-pipeline/
├── docker-compose.yml              # Dev single-node setup
├── docker-compose.prod.yml         # Multi-node production setup
├── config/
│   ├── kafka/
│   │   ├── kraft/server.properties
│   │   └── log4j.properties
│   ├── connect/
│   │   └── connect-distributed.properties
│   └── schema-registry/
│       └── schema-registry.properties
├── connectors/
│   ├── source/
│   │   ├── mysql-cdc-source.json
│   │   └── postgres-cdc-source.json
│   └── sink/
│       ├── postgres-sink.json
│       └── mysql-sink.json
├── scripts/
│   ├── setup.sh
│   ├── create-topics.sh
│   └── health-check.sh
└── monitoring/
    ├── prometheus.yml
    └── grafana/
        └── dashboards/
```

---

## Option 1: Single-Node Dev (Docker Compose + KRaft)

### `docker-compose.yml`

```yaml
version: "3.9"

services:

  kafka:
    image: confluentinc/cp-kafka:7.6.1
    hostname: kafka
    container_name: kafka
    ports:
      - "9092:9092"       # External client access
      - "9093:9093"       # Controller (KRaft internal)
      - "7071:7071"       # JMX for Prometheus
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: "broker,controller"
      KAFKA_CONTROLLER_QUORUM_VOTERS: "1@kafka:9093"
      KAFKA_LISTENERS: "PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093"
      KAFKA_ADVERTISED_LISTENERS: "PLAINTEXT://localhost:9092"
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT"
      KAFKA_CONTROLLER_LISTENER_NAMES: "CONTROLLER"
      KAFKA_INTER_BROKER_LISTENER_NAME: "PLAINTEXT"
      KAFKA_LOG_DIRS: "/var/lib/kafka/data"
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
      KAFKA_NUM_PARTITIONS: 3
      KAFKA_DEFAULT_REPLICATION_FACTOR: 1
      KAFKA_MIN_INSYNC_REPLICAS: 1
      KAFKA_LOG_RETENTION_HOURS: 168           # 7 days
      KAFKA_LOG_SEGMENT_BYTES: 1073741824      # 1 GB
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_JMX_PORT: 7071
      KAFKA_JMX_HOSTNAME: kafka
      CLUSTER_ID: "MkU3OEVBNTcwNTJENDM2Qg"    # Generate with: kafka-storage random-uuid
    volumes:
      - kafka-data:/var/lib/kafka/data
    healthcheck:
      test: kafka-broker-api-versions --bootstrap-server localhost:9092
      interval: 30s
      timeout: 10s
      retries: 5

  schema-registry:
    image: confluentinc/cp-schema-registry:7.6.1
    hostname: schema-registry
    container_name: schema-registry
    depends_on:
      kafka:
        condition: service_healthy
    ports:
      - "8081:8081"
    environment:
      SCHEMA_REGISTRY_HOST_NAME: schema-registry
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: "kafka:9092"
      SCHEMA_REGISTRY_LISTENERS: "http://0.0.0.0:8081"
      SCHEMA_REGISTRY_KAFKASTORE_TOPIC: "_schemas"
      SCHEMA_REGISTRY_SCHEMA_COMPATIBILITY_LEVEL: "BACKWARD"
    healthcheck:
      test: curl -f http://localhost:8081/subjects || exit 1
      interval: 30s
      timeout: 10s
      retries: 5

  kafka-connect:
    image: debezium/connect:2.6
    hostname: kafka-connect
    container_name: kafka-connect
    depends_on:
      kafka:
        condition: service_healthy
      schema-registry:
        condition: service_healthy
    ports:
      - "8083:8083"
    environment:
      BOOTSTRAP_SERVERS: "kafka:9092"
      GROUP_ID: "kafka-connect-cluster"
      CONFIG_STORAGE_TOPIC: "_connect-configs"
      OFFSET_STORAGE_TOPIC: "_connect-offsets"
      STATUS_STORAGE_TOPIC: "_connect-status"
      CONFIG_STORAGE_REPLICATION_FACTOR: 1
      OFFSET_STORAGE_REPLICATION_FACTOR: 1
      STATUS_STORAGE_REPLICATION_FACTOR: 1
      KEY_CONVERTER: "org.apache.kafka.connect.json.JsonConverter"
      VALUE_CONVERTER: "org.apache.kafka.connect.json.JsonConverter"
      KEY_CONVERTER_SCHEMA_REGISTRY_URL: "http://schema-registry:8081"
      VALUE_CONVERTER_SCHEMA_REGISTRY_URL: "http://schema-registry:8081"
      CONNECT_REST_ADVERTISED_HOST_NAME: kafka-connect
      CONNECT_PLUGIN_PATH: "/kafka/connect,/usr/share/confluent-hub-components"
    volumes:
      - ./connectors:/kafka/connect/custom
    healthcheck:
      test: curl -f http://localhost:8083/ || exit 1
      interval: 30s
      timeout: 10s
      retries: 10

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    container_name: kafka-ui
    depends_on:
      - kafka
      - schema-registry
      - kafka-connect
    ports:
      - "8090:8080"
    environment:
      KAFKA_CLUSTERS_0_NAME: local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:9092
      KAFKA_CLUSTERS_0_SCHEMAREGISTRY: http://schema-registry:8081
      KAFKA_CLUSTERS_0_KAFKACONNECT_0_NAME: connect
      KAFKA_CLUSTERS_0_KAFKACONNECT_0_ADDRESS: http://kafka-connect:8083

  mysql:
    image: mysql:8.0
    container_name: mysql-source
    ports:
      - "3307:3306"
    environment:
      MYSQL_ROOT_PASSWORD: rootpassword
      MYSQL_DATABASE: sourcedb
      MYSQL_USER: kafka_user
      MYSQL_PASSWORD: kafka_password
    command: >
      --log-bin=mysql-bin
      --binlog-format=ROW
      --binlog-row-image=FULL
      --gtid-mode=ON
      --enforce-gtid-consistency=ON
      --server-id=1
    volumes:
      - mysql-data:/var/lib/mysql
      - ./scripts/mysql-init.sql:/docker-entrypoint-initdb.d/init.sql

  postgres:
    image: postgres:16
    container_name: postgres-target
    ports:
      - "5432:5432"
    environment:
      POSTGRES_DB: targetdb
      POSTGRES_USER: kafka_user
      POSTGRES_PASSWORD: kafka_password
    command: >
      postgres
      -c wal_level=logical
      -c max_wal_senders=10
      -c max_replication_slots=10
    volumes:
      - postgres-data:/var/lib/postgresql/data
      - ./scripts/postgres-init.sql:/docker-entrypoint-initdb.d/init.sql

  prometheus:
    image: prom/prometheus:latest
    container_name: prometheus
    ports:
      - "9090:9090"
    volumes:
      - ./monitoring/prometheus.yml:/etc/prometheus/prometheus.yml

  grafana:
    image: grafana/grafana:latest
    container_name: grafana
    ports:
      - "3000:3000"
    environment:
      GF_SECURITY_ADMIN_PASSWORD: admin
    volumes:
      - grafana-data:/var/lib/grafana
      - ./monitoring/grafana/dashboards:/var/lib/grafana/dashboards

volumes:
  kafka-data:
  mysql-data:
  postgres-data:
  grafana-data:
```

---

## Option 2: Multi-Node Production (KRaft, 3 Brokers)

### `docker-compose.prod.yml` (abbreviated — use Kubernetes for real prod)

```yaml
version: "3.9"

services:

  kafka-controller-1:
    image: confluentinc/cp-kafka:7.6.1
    hostname: kafka-controller-1
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: "controller"
      KAFKA_CONTROLLER_QUORUM_VOTERS: "1@kafka-controller-1:9093,2@kafka-controller-2:9093,3@kafka-controller-3:9093"
      KAFKA_LISTENERS: "CONTROLLER://0.0.0.0:9093"
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: "CONTROLLER:PLAINTEXT"
      KAFKA_CONTROLLER_LISTENER_NAMES: "CONTROLLER"
      CLUSTER_ID: "REPLACE_WITH_GENERATED_UUID"

  kafka-controller-2:
    image: confluentinc/cp-kafka:7.6.1
    hostname: kafka-controller-2
    environment:
      KAFKA_NODE_ID: 2
      KAFKA_PROCESS_ROLES: "controller"
      KAFKA_CONTROLLER_QUORUM_VOTERS: "1@kafka-controller-1:9093,2@kafka-controller-2:9093,3@kafka-controller-3:9093"
      KAFKA_LISTENERS: "CONTROLLER://0.0.0.0:9093"
      CLUSTER_ID: "REPLACE_WITH_GENERATED_UUID"

  kafka-controller-3:
    image: confluentinc/cp-kafka:7.6.1
    hostname: kafka-controller-3
    environment:
      KAFKA_NODE_ID: 3
      KAFKA_PROCESS_ROLES: "controller"
      KAFKA_CONTROLLER_QUORUM_VOTERS: "1@kafka-controller-1:9093,2@kafka-controller-2:9093,3@kafka-controller-3:9093"
      KAFKA_LISTENERS: "CONTROLLER://0.0.0.0:9093"
      CLUSTER_ID: "REPLACE_WITH_GENERATED_UUID"

  kafka-broker-1:
    image: confluentinc/cp-kafka:7.6.1
    hostname: kafka-broker-1
    depends_on:
      - kafka-controller-1
      - kafka-controller-2
      - kafka-controller-3
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 4
      KAFKA_PROCESS_ROLES: "broker"
      KAFKA_CONTROLLER_QUORUM_VOTERS: "1@kafka-controller-1:9093,2@kafka-controller-2:9093,3@kafka-controller-3:9093"
      KAFKA_LISTENERS: "PLAINTEXT://0.0.0.0:9092,INTERNAL://0.0.0.0:29092"
      KAFKA_ADVERTISED_LISTENERS: "PLAINTEXT://kafka-broker-1:9092,INTERNAL://kafka-broker-1:29092"
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: "PLAINTEXT:PLAINTEXT,INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT"
      KAFKA_INTER_BROKER_LISTENER_NAME: "INTERNAL"
      KAFKA_CONTROLLER_LISTENER_NAMES: "CONTROLLER"
      KAFKA_DEFAULT_REPLICATION_FACTOR: 3
      KAFKA_MIN_INSYNC_REPLICAS: 2
      KAFKA_NUM_PARTITIONS: 6
      KAFKA_LOG_RETENTION_HOURS: 168
      CLUSTER_ID: "REPLACE_WITH_GENERATED_UUID"
    volumes:
      - kafka-broker-1-data:/var/lib/kafka/data

  kafka-broker-2:
    image: confluentinc/cp-kafka:7.6.1
    hostname: kafka-broker-2
    environment:
      KAFKA_NODE_ID: 5
      KAFKA_PROCESS_ROLES: "broker"
      KAFKA_CONTROLLER_QUORUM_VOTERS: "1@kafka-controller-1:9093,2@kafka-controller-2:9093,3@kafka-controller-3:9093"
      KAFKA_ADVERTISED_LISTENERS: "PLAINTEXT://kafka-broker-2:9092,INTERNAL://kafka-broker-2:29092"
      KAFKA_DEFAULT_REPLICATION_FACTOR: 3
      KAFKA_MIN_INSYNC_REPLICAS: 2
      CLUSTER_ID: "REPLACE_WITH_GENERATED_UUID"
    volumes:
      - kafka-broker-2-data:/var/lib/kafka/data

  kafka-broker-3:
    image: confluentinc/cp-kafka:7.6.1
    hostname: kafka-broker-3
    environment:
      KAFKA_NODE_ID: 6
      KAFKA_PROCESS_ROLES: "broker"
      KAFKA_CONTROLLER_QUORUM_VOTERS: "1@kafka-controller-1:9093,2@kafka-controller-2:9093,3@kafka-controller-3:9093"
      KAFKA_ADVERTISED_LISTENERS: "PLAINTEXT://kafka-broker-3:9092,INTERNAL://kafka-broker-3:29092"
      KAFKA_DEFAULT_REPLICATION_FACTOR: 3
      KAFKA_MIN_INSYNC_REPLICAS: 2
      CLUSTER_ID: "REPLACE_WITH_GENERATED_UUID"
    volumes:
      - kafka-broker-3-data:/var/lib/kafka/data

  schema-registry:
    image: confluentinc/cp-schema-registry:7.6.1
    depends_on:
      - kafka-broker-1
      - kafka-broker-2
      - kafka-broker-3
    ports:
      - "8081:8081"
    environment:
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: "kafka-broker-1:9092,kafka-broker-2:9092,kafka-broker-3:9092"
      SCHEMA_REGISTRY_HOST_NAME: schema-registry
      SCHEMA_REGISTRY_SCHEMA_COMPATIBILITY_LEVEL: "BACKWARD"

volumes:
  kafka-broker-1-data:
  kafka-broker-2-data:
  kafka-broker-3-data:
```

---

## Initial Setup Scripts

### `scripts/setup.sh`

```bash
#!/bin/bash
set -e

echo "Starting Kafka pipeline stack..."
docker-compose up -d

echo "Waiting for services to be healthy..."
sleep 30

echo "Creating Kafka topics..."
bash scripts/create-topics.sh

echo "Setup complete. UI available at http://localhost:8090"
```

### `scripts/create-topics.sh`

```bash
#!/bin/bash

KAFKA_CONTAINER="kafka"
BOOTSTRAP="localhost:9092"

create_topic() {
  local topic=$1
  local partitions=${2:-3}
  local replication=${3:-1}
  local retention_ms=${4:-604800000}  # 7 days

  docker exec $KAFKA_CONTAINER kafka-topics \
    --create \
    --bootstrap-server $BOOTSTRAP \
    --topic "$topic" \
    --partitions $partitions \
    --replication-factor $replication \
    --config retention.ms=$retention_ms \
    --if-not-exists

  echo "Created topic: $topic"
}

# CDC topics
create_topic "prod.mysql.sourcedb.orders"     3 1
create_topic "prod.mysql.sourcedb.customers"  3 1
create_topic "prod.mysql.sourcedb.products"   3 1

# DLQ topics
create_topic "prod.dlq.errors"  1 1 -1     # Retain forever

# Internal topics (Kafka Connect)
create_topic "_connect-configs"  1 1
create_topic "_connect-offsets"  25 1
create_topic "_connect-status"   5 1

echo "All topics created."
docker exec $KAFKA_CONTAINER kafka-topics --list --bootstrap-server $BOOTSTRAP
```

### `scripts/mysql-init.sql`

```sql
-- Grant CDC permissions to kafka_user
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
```

### `scripts/postgres-init.sql`

```sql
-- Grant permissions
GRANT ALL PRIVILEGES ON DATABASE targetdb TO kafka_user;

-- Create target schema
CREATE SCHEMA IF NOT EXISTS pipeline;

-- Target tables (match source structure)
CREATE TABLE IF NOT EXISTS pipeline.orders (
    id          BIGINT PRIMARY KEY,
    customer_id BIGINT,
    product_id  BIGINT,
    quantity    INT,
    amount      NUMERIC(10,2),
    status      VARCHAR(50),
    created_at  TIMESTAMP,
    updated_at  TIMESTAMP,
    _cdc_op     VARCHAR(10),        -- CDC operation: INSERT/UPDATE/DELETE
    _ingested_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS pipeline.customers (
    id         BIGINT PRIMARY KEY,
    name       VARCHAR(255),
    email      VARCHAR(255),
    phone      VARCHAR(50),
    created_at TIMESTAMP,
    _cdc_op    VARCHAR(10),
    _ingested_at TIMESTAMP DEFAULT NOW()
);
```

---

## Health Check

### `scripts/health-check.sh`

```bash
#!/bin/bash

check() {
  local name=$1
  local url=$2
  if curl -sf "$url" > /dev/null 2>&1; then
    echo "✓ $name is healthy"
  else
    echo "✗ $name is NOT healthy at $url"
  fi
}

check "Kafka"           "http://localhost:9092"
check "Schema Registry" "http://localhost:8081/subjects"
check "Kafka Connect"   "http://localhost:8083/"
check "Kafka UI"        "http://localhost:8090"
check "Prometheus"      "http://localhost:9090/-/healthy"
check "Grafana"         "http://localhost:3000/api/health"

echo ""
echo "Kafka topics:"
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

echo ""
echo "Kafka Connect connectors:"
curl -s http://localhost:8083/connectors | python3 -m json.tool
```

---

## KRaft Cluster ID Generation

```bash
# Generate a new cluster ID (only needed once)
docker run --rm confluentinc/cp-kafka:7.6.1 kafka-storage random-uuid

# Format storage with cluster ID (done automatically in Docker env via CLUSTER_ID env var)
kafka-storage format -t <cluster-id> -c /etc/kafka/kraft/server.properties
```

---

## Useful Kafka CLI Commands

```bash
# List topics
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Describe a topic
docker exec kafka kafka-topics --describe \
  --topic prod.mysql.sourcedb.orders \
  --bootstrap-server localhost:9092

# Watch consumer group lag
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe \
  --group connect-mysql-cdc-source

# Consume from a topic (from beginning)
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic prod.mysql.sourcedb.orders \
  --from-beginning \
  --max-messages 10

# List all consumer groups
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --list

# Check Schema Registry subjects
curl http://localhost:8081/subjects

# Check connector status
curl http://localhost:8083/connectors/mysql-cdc-source/status | python3 -m json.tool
```
