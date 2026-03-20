# Environment Setup

## Why This Document Exists

Before running `docker compose up -d`, several configuration files must exist on disk. Docker mounts them at container startup — if they're missing, Prometheus fails to start, MySQL tables are never created, and the pipeline cannot run. This document walks through every file that must be created, with the correct content for both Option A (Docker databases) and Option B (VPS databases).

---

## Directory Structure

**Why:** Keeping connectors, scripts, and monitoring configs in separate folders makes the project navigable and lets Docker Compose mount specific directories cleanly.

**Windows PowerShell:**

```powershell
mkdir connectors\source
mkdir connectors\sink
mkdir scripts
mkdir monitoring\grafana\dashboards
mkdir config
mkdir consumers
```

**Linux (AlmaLinux 9) / macOS (M1/M2/M3):**

```bash
mkdir -p connectors/source connectors/sink scripts monitoring/grafana/dashboards config consumers
```

Expected structure:

```
kafka-pipeline/
├── connectors/
│   ├── source/         ← source connector JSON configs
│   └── sink/           ← sink connector JSON configs
├── scripts/            ← SQL init files, Python scripts
├── consumers/          ← Python consumer scripts
├── monitoring/
│   ├── prometheus.yml
│   └── grafana/
│       └── dashboards/
├── config/
├── docker-compose.yml
└── requirements.txt
```

---

## Required Files — Create These Before `docker compose up -d`

### File checklist

- [ ] `docker-compose.yml`
- [ ] `scripts/mysql-init.sql`
- [ ] `scripts/postgres-init.sql`
- [ ] `monitoring/prometheus.yml`
- [ ] `connectors/source/mysql-cdc-source.json`
- [ ] `connectors/sink/postgres-sink.json`

> **Why:** `docker compose up -d` mounts `monitoring/prometheus.yml` at startup. If the file is missing, the Prometheus container fails immediately with a "no such file" mount error. MySQL and PostgreSQL init scripts also run at first container start only — if they're missing, source tables and schema grants are never created.

---

## `scripts/mysql-init.sql`

**Why:** When the Docker MySQL container starts for the first time, it automatically runs any `.sql` files in `/docker-entrypoint-initdb.d/`. This script creates the source tables and grants CDC permissions to `kafka_user`. Without the `REPLICATION SLAVE` grant, Debezium cannot read the binlog.

**What happens:** On first container start, MySQL executes this script as root. `kafka_user` (created via `MYSQL_USER` in docker-compose) receives binlog read permissions and owns the `orders`/`customers` tables. Sample data is inserted so the initial snapshot has rows to capture.

```sql
-- Grant CDC permissions to kafka_user (used by Debezium to read binlog)
-- REPLICATION SLAVE and REPLICATION CLIENT are the key permissions
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'kafka_user'@'%';
FLUSH PRIVILEGES;

-- Source tables
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

-- Sample data so the initial snapshot has something to capture
INSERT INTO customers (name, email, phone) VALUES
  ('Alice Johnson', 'alice@example.com', '+1-555-0101'),
  ('Bob Smith',     'bob@example.com',   '+1-555-0102');

INSERT INTO orders (customer_id, product_id, quantity, amount, status) VALUES
  (1, 101, 2, 49.99, 'COMPLETED'),
  (2, 102, 1, 99.00, 'PENDING');
```

Save as: `scripts/mysql-init.sql`

---

## `scripts/postgres-init.sql`

**Why:** This script runs automatically when the Docker PostgreSQL container starts for the first time. It creates the `pipeline` schema where Debezium will write synced data and grants the necessary privileges to `kafka_user`.

**What happens:** The schema `pipeline` is created under `targetdb`. Debezium's JDBC Sink connector will then auto-create `pipeline.orders` and `pipeline.customers` via `schema.evolution: basic`.

> **Important:** Do NOT create the `pipeline.orders` and `pipeline.customers` tables manually here. If you pre-create them with different columns (e.g., `_cdc_op`, `_ingested_at`), the Debezium JDBC Sink connector will fail when it tries to `ALTER TABLE` to add its own columns. Let Debezium create the tables automatically.

```sql
-- Grant schema permissions to kafka_user
GRANT ALL PRIVILEGES ON DATABASE targetdb TO kafka_user;

-- Create target schema — Debezium will create tables automatically inside this schema
CREATE SCHEMA IF NOT EXISTS pipeline;
GRANT ALL PRIVILEGES ON SCHEMA pipeline TO kafka_user;
```

Save as: `scripts/postgres-init.sql`

---

## `monitoring/prometheus.yml`

**Why:** Prometheus needs a config file telling it where to scrape metrics from. Without this file, the Prometheus container fails to start with a "file not found" mount error.

**What happens:** Prometheus scrapes `kafka-exporter:9308` for consumer lag metrics and `kafka-connect:7072` for connector metrics. The `kafka-connect` target will show as DOWN (expected — `debezium/connect:2.6` does not expose a Prometheus endpoint on port 7072). The `kafka-exporter` target will be UP and provide consumer lag data.

```yaml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  - job_name: "kafka-exporter"
    static_configs:
      - targets: ["kafka-exporter:9308"]

  - job_name: "kafka-connect"
    static_configs:
      - targets: ["kafka-connect:7072"]
    metrics_path: /metrics
```

Save as: `monitoring/prometheus.yml`

---

## `docker-compose.yml`

**Why:** This is the master file that defines all containers, their dependencies, ports, environment variables, and health checks. Docker Compose reads it and orchestrates everything.

**Critical notes for Docker 29.x / Compose v5.x:**
- **No `version:` field** — the `version: "3.9"` field is deprecated in Compose v5.x and causes a warning. The field is omitted entirely.
- **`kafka` service name is used as hostname** — inside the Docker network, all containers reference Kafka as `kafka:9092`. Your host machine accesses it as `localhost:9092`. `KAFKA_ADVERTISED_LISTENERS` must say `kafka:9092`, not `localhost:9092`.
- **`JsonConverter` not `AvroConverter`** — the `debezium/connect:2.6` image does NOT include Confluent Avro serializer JARs. Using `AvroConverter` causes `ClassNotFoundException`. Use `JsonConverter` throughout.
- **Healthchecks with `condition: service_healthy`** — `kafka-connect` waits for `kafka` to be healthy before starting. Without this, Connect starts before Kafka is ready and crashes.

---

### Option A — Docker databases (MySQL + PostgreSQL as containers)

**What:** Use this if you want everything local with no external server needed. All 9 services run as Docker containers.

**What happens:** On first `docker compose up -d`, Docker downloads all images (~3–5 GB total), creates named volumes for persistent storage, and starts all services in dependency order. `kafka-connect` takes the longest (~2–3 minutes) to become healthy.

```yaml
services:

  kafka:
    image: confluentinc/cp-kafka:7.6.1
    hostname: kafka
    container_name: kafka
    ports:
      - "9092:9092"
      - "9093:9093"
      - "7071:7071"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: "broker,controller"
      KAFKA_CONTROLLER_QUORUM_VOTERS: "1@kafka:9093"
      KAFKA_LISTENERS: "PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093"
      KAFKA_ADVERTISED_LISTENERS: "PLAINTEXT://kafka:9092"
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT"
      KAFKA_CONTROLLER_LISTENER_NAMES: "CONTROLLER"
      KAFKA_INTER_BROKER_LISTENER_NAME: "PLAINTEXT"
      KAFKA_LOG_DIRS: "/var/lib/kafka/data"
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
      KAFKA_NUM_PARTITIONS: 3
      KAFKA_DEFAULT_REPLICATION_FACTOR: 1
      KAFKA_MIN_INSYNC_REPLICAS: 1
      KAFKA_LOG_RETENTION_HOURS: 168
      KAFKA_LOG_SEGMENT_BYTES: 1073741824
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_JMX_PORT: 7071
      KAFKA_JMX_HOSTNAME: kafka
      CLUSTER_ID: "MkU3OEVBNTcwNTJENDM2Qg"
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
      KEY_CONVERTER_SCHEMAS_ENABLE: "false"
      VALUE_CONVERTER_SCHEMAS_ENABLE: "false"
      CONNECT_REST_ADVERTISED_HOST_NAME: kafka-connect
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

  kafka-exporter:
    image: danielqsj/kafka-exporter:latest
    container_name: kafka-exporter
    command:
      - "--kafka.server=kafka:9092"
      - "--web.listen-address=:9308"
      - "--topic.filter=.*"
      - "--group.filter=.*"
    ports:
      - "9308:9308"
    depends_on:
      kafka:
        condition: service_healthy

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

### Option B — VPS databases (no MySQL/PostgreSQL containers)

**What:** Use this if MySQL and PostgreSQL run on a remote VPS. Remove the `mysql` and `postgres` service blocks entirely and update the volumes section accordingly.

**What happens:** Kafka Connect connects to your VPS databases directly. The connector JSON files must use the VPS IP and credentials (see Phase 3 in `00-implementation-plan.md` for VPS pre-configuration steps).

```yaml
services:

  kafka:
    image: confluentinc/cp-kafka:7.6.1
    hostname: kafka
    container_name: kafka
    ports:
      - "9092:9092"
      - "9093:9093"
      - "7071:7071"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: "broker,controller"
      KAFKA_CONTROLLER_QUORUM_VOTERS: "1@kafka:9093"
      KAFKA_LISTENERS: "PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093"
      KAFKA_ADVERTISED_LISTENERS: "PLAINTEXT://kafka:9092"
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT"
      KAFKA_CONTROLLER_LISTENER_NAMES: "CONTROLLER"
      KAFKA_INTER_BROKER_LISTENER_NAME: "PLAINTEXT"
      KAFKA_LOG_DIRS: "/var/lib/kafka/data"
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
      KAFKA_NUM_PARTITIONS: 3
      KAFKA_DEFAULT_REPLICATION_FACTOR: 1
      KAFKA_MIN_INSYNC_REPLICAS: 1
      KAFKA_LOG_RETENTION_HOURS: 168
      KAFKA_LOG_SEGMENT_BYTES: 1073741824
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_JMX_PORT: 7071
      KAFKA_JMX_HOSTNAME: kafka
      CLUSTER_ID: "MkU3OEVBNTcwNTJENDM2Qg"
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
      KEY_CONVERTER_SCHEMAS_ENABLE: "false"
      VALUE_CONVERTER_SCHEMAS_ENABLE: "false"
      CONNECT_REST_ADVERTISED_HOST_NAME: kafka-connect
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

  kafka-exporter:
    image: danielqsj/kafka-exporter:latest
    container_name: kafka-exporter
    command:
      - "--kafka.server=kafka:9092"
      - "--web.listen-address=:9308"
      - "--topic.filter=.*"
      - "--group.filter=.*"
    ports:
      - "9308:9308"
    depends_on:
      kafka:
        condition: service_healthy

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

# NOTE: No mysql or postgres services — using VPS databases
volumes:
  kafka-data:
  grafana-data:
```

---

## Port Map

| Service | Host Port | Container Port | URL |
|---------|-----------|---------------|-----|
| Kafka broker | 9092 | 9092 | (no HTTP) |
| Kafka controller | 9093 | 9093 | (internal only) |
| Kafka JMX | 7071 | 7071 | (metrics) |
| Schema Registry | 8081 | 8081 | http://localhost:8081 |
| Kafka Connect | 8083 | 8083 | http://localhost:8083 |
| Kafka UI | **8090** | 8080 | http://localhost:8090 |
| Prometheus | 9090 | 9090 | http://localhost:9090 |
| Grafana | 3000 | 3000 | http://localhost:3000 |
| Kafka Exporter | 9308 | 9308 | http://localhost:9308/metrics |
| MySQL (Docker) | **3307** | 3306 | localhost:3307 |
| PostgreSQL (Docker) | 5432 | 5432 | localhost:5432 |

> **Why 8090 for Kafka UI?** Port 8080 is commonly used by local development servers (Tomcat, Spring Boot). Using 8090 avoids conflicts.
>
> **Why 3307 for MySQL?** Port 3306 may already be used by a local MySQL installation on Windows. Using 3307 as the host port avoids the `bind: address already in use` error.

---

## Start All Containers

**Why:** `docker compose up -d` starts all services defined in `docker-compose.yml` in detached mode (background). On first run it downloads images and creates volumes (~15 min on slow connections). Subsequent starts take ~60 seconds.

Run from your project root directory (where `docker-compose.yml` is):

**Windows PowerShell:**

```powershell
docker compose up -d
```

**Linux (AlmaLinux 9) / macOS (M1/M2/M3):**

```bash
docker compose up -d
```

Watch startup progress:

```bash
docker compose ps
```

Wait until all services show `healthy` or `running`. `kafka-connect` takes the longest (~2–3 minutes).

---

## Verify All Services Are Healthy

```bash
docker compose ps
```

Expected output (Option A — 9 containers):

```
NAME              IMAGE                                    STATUS
kafka             confluentinc/cp-kafka:7.6.1              Up (healthy)
schema-registry   confluentinc/cp-schema-registry:7.6.1   Up (healthy)
kafka-connect     debezium/connect:2.6                     Up (healthy)
kafka-ui          provectuslabs/kafka-ui:latest            Up
mysql-source      mysql:8.0                                Up
postgres-target   postgres:16                              Up
kafka-exporter    danielqsj/kafka-exporter:latest          Up
prometheus        prom/prometheus:latest                   Up
grafana           grafana/grafana:latest                   Up
```

Verify key endpoints:

**Linux (AlmaLinux 9) / macOS (M1/M2/M3):**

```bash
curl http://localhost:8081/subjects    # returns []
curl http://localhost:8083/            # returns {"version":"..."}
```

**Windows PowerShell:**

```powershell
# Schema Registry — should return []
curl.exe http://localhost:8081/subjects

# Kafka Connect — should return JSON with version info
curl.exe http://localhost:8083/

# Open Kafka UI in browser
Start-Process "http://localhost:8090"
```

> **Why `curl.exe` on Windows PowerShell?**
> In PowerShell, `curl` is an alias for `Invoke-WebRequest` which shows verbose HTML output and may prompt for credentials. Always use `curl.exe` (the real curl binary) for API calls in PowerShell. On Linux and macOS, `curl` works as-is.

---

## Multi-Node Production Setup (Reference)

For production, run 3 Kafka brokers with separate controller nodes. The following is an abbreviated reference — for real production use Kubernetes with the Strimzi or Confluent operators.

```yaml
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
      CLUSTER_ID: "REPLACE_WITH_GENERATED_UUID"
    volumes:
      - kafka-broker-1-data:/var/lib/kafka/data

volumes:
  kafka-broker-1-data:
```

Generate a cluster UUID:

```bash
docker run --rm confluentinc/cp-kafka:7.6.1 kafka-storage random-uuid
```

---

## Useful Kafka CLI Commands

All commands below work the same on Windows PowerShell, Linux, and macOS (they run inside the container via `docker exec`):

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
```

Check Schema Registry and connector status:

**Linux (AlmaLinux 9) / macOS (M1/M2/M3):**

```bash
curl http://localhost:8081/subjects
curl http://localhost:8083/connectors/mysql-cdc-source/status | python3 -m json.tool
```

**Windows PowerShell:**

```powershell
curl.exe http://localhost:8081/subjects
curl.exe http://localhost:8083/connectors/mysql-cdc-source/status | python3 -m json.tool
```

---

## Health Check Script

**Why:** Quickly verify all services are up before registering connectors.

**Note:** This script is for Linux (AlmaLinux 9) and macOS. See the Windows PowerShell equivalent below.

### `scripts/health-check.sh` (Linux / macOS)

```bash
#!/bin/bash

check() {
  local name=$1
  local url=$2
  if curl -sf "$url" > /dev/null 2>&1; then
    echo "OK  $name"
  else
    echo "FAIL $name is NOT healthy at $url"
  fi
}

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

### Windows PowerShell health check

```powershell
$services = @{
    "Schema Registry" = "http://localhost:8081/subjects"
    "Kafka Connect"   = "http://localhost:8083/"
    "Kafka UI"        = "http://localhost:8090"
    "Prometheus"      = "http://localhost:9090/-/healthy"
    "Grafana"         = "http://localhost:3000/api/health"
}

foreach ($name in $services.Keys) {
    try {
        $null = curl.exe -sf $services[$name] 2>$null
        if ($LASTEXITCODE -eq 0) {
            Write-Host "OK  $name"
        } else {
            Write-Host "FAIL $name"
        }
    } catch {
        Write-Host "FAIL $name"
    }
}

Write-Host "`nKafka topics:"
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

Write-Host "`nKafka Connect connectors:"
curl.exe -s http://localhost:8083/connectors
```

---

## Common Errors

**Error: `port 3306 already in use`**
- Your local MySQL is running on port 3306.
- Fix: The docker-compose.yml above already maps MySQL to `3307:3306` on the host. If you changed it back to `3306:3306`, revert to `3307:3306`.

**Error: `port 8080 already in use`**
- Another service (Spring Boot, Tomcat) is using port 8080.
- Fix: The docker-compose.yml above already maps Kafka UI to `8090:8080`.

**Error: `monitoring/prometheus.yml: no such file or directory`**
- The prometheus volume mount fails if the file doesn't exist on the host.
- Fix: Create `monitoring/prometheus.yml` first, then run `docker compose up -d`.

**Error: `CLUSTER_ID mismatch` on restart**
- Kafka's data volume has a different cluster ID from `docker-compose.yml`.
- Fix: `docker compose down -v` (removes volumes), then `docker compose up -d`.

**kafka-exporter keeps restarting**
- It started before Kafka was fully ready.
- Fix: `docker compose restart kafka-exporter` after Kafka shows `healthy`.

**Error: `TLS handshake timeout` during image pull**
- Network issue. Retry `docker compose up -d` — Docker resumes from cached layers. Or pull images one at a time first:

```bash
docker pull confluentinc/cp-kafka:7.6.1
docker pull confluentinc/cp-schema-registry:7.6.1
docker pull debezium/connect:2.6
docker pull provectuslabs/kafka-ui:latest
docker pull danielqsj/kafka-exporter:latest
docker pull prom/prometheus:latest
docker pull grafana/grafana:latest
docker pull mysql:8.0
docker pull postgres:16
```
