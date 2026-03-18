# Kafka Pipeline — Implementation Plan

**Target audience:** Developer starting from scratch on Windows 11 with Python 3.13, Docker Desktop 29.2.1, Docker Compose v5.1.0, and a venv already set up.

**Goal:** A fully working MySQL → Kafka → PostgreSQL CDC pipeline with monitoring, running locally in Docker.

**Duration:** 4 days (split across 8 phases)

---

## Prerequisites Checklist

Before you begin, confirm the following:

- [ ] Python 3.13 installed (`python --version`)
- [ ] Docker Desktop 29.2.1 running (`docker --version`)
- [ ] Docker Compose v5.1.0 available (`docker compose version`)
- [ ] A Python venv already created for this project
- [ ] At least **8 GB free RAM** and **10 GB free disk** for Docker containers

---

## Architecture Overview

```
MySQL (source)
    │  binlog (ROW format)
    ▼
Debezium MySQL Source Connector  (inside Kafka Connect)
    │  Avro records
    ▼
Kafka Cluster  +  Schema Registry
    │  Avro records
    ▼
JDBC Sink Connector  (inside Kafka Connect)
    │  JDBC
    ▼
PostgreSQL (target)

Monitoring: Prometheus + Grafana + Kafka UI + Kafka Exporter
```

---

## Port Map (Quick Reference)

| Service | Port | URL |
|---------|------|-----|
| Kafka (broker) | 9092 | — |
| Schema Registry | 8081 | http://localhost:8081 |
| Kafka Connect | 8083 | http://localhost:8083 |
| Kafka UI | 8090 | http://localhost:8090 |
| MySQL | 3307 | — |
| PostgreSQL | 5432 | — |
| Prometheus | 9090 | http://localhost:9090 |
| Grafana | 3000 | http://localhost:3000 |
| Kafka Exporter | 9308 | http://localhost:9308/metrics |

---

---

# Phase 1: Project Setup (Day 1)

**Time estimate:** 20–30 minutes

---

## Step 1.1 — Create the folder structure

Open PowerShell (or Git Bash) in the directory where you want to create the project.

```powershell
# Create project root and all subdirectories in one shot
mkdir kafka-pipeline
cd kafka-pipeline

mkdir connectors\source
mkdir connectors\sink
mkdir scripts
mkdir monitoring\grafana\dashboards
mkdir config
```

Verify the structure:

```powershell
# PowerShell tree
Get-ChildItem -Recurse -Directory | Select-Object FullName
```

Expected layout:

```
kafka-pipeline/
├── connectors/
│   ├── source/
│   └── sink/
├── scripts/
├── monitoring/
│   └── grafana/
│       └── dashboards/
└── config/
```

- [ ] Folder structure created

---

## Step 1.2 — Activate your venv

```powershell
# Windows PowerShell — assuming venv is named .venv at project root
.\.venv\Scripts\Activate.ps1

# Or if you named it venv
.\venv\Scripts\Activate.ps1

# Git Bash alternative
source .venv/Scripts/activate
```

Your prompt should now show `(.venv)` at the start.

- [ ] venv activated

---

## Step 1.3 — Install Python dependencies

```powershell
pip install confluent-kafka psycopg2-binary mysql-connector-python
pip freeze > requirements.txt
```

- [ ] Python dependencies installed

---

## Step 1.4 — Create `requirements.txt` (if not using pip freeze)

If you prefer to write it manually:

```
confluent-kafka==2.4.0
psycopg2-binary==2.9.9
mysql-connector-python==8.3.0
```

---

## Phase 1 — Verification

- [ ] `kafka-pipeline/` directory exists with the correct sub-folders
- [ ] `(.venv)` appears in your shell prompt
- [ ] `python -c "import confluent_kafka; print('OK')"` prints `OK`

---

---

# Phase 2: Docker Infrastructure (Day 1)

**Time estimate:** 30–45 minutes (plus image download time, ~15 min on first run)

---

## Step 2.1 — Create `docker-compose.yml`

Create `kafka-pipeline/docker-compose.yml` with the following content.

**Important notes for Docker 29.x:**
- No `version:` field — it is deprecated and causes a warning on Docker Compose v5.x
- Uses `KAFKA_ADVERTISED_LISTENERS: "PLAINTEXT://kafka:9092"` for inter-container communication. The `kafka-ui` and connector containers reference `kafka:9092`, not `localhost:9092`.
- Healthchecks ensure services start in the correct order.

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

- [ ] `docker-compose.yml` created

---

## Step 2.2 — Create `scripts/mysql-init.sql`

This runs automatically when the MySQL container first starts. It grants CDC permissions to the `kafka_user` and creates sample tables.

```sql
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
```

- [ ] `scripts/mysql-init.sql` created

---

## Step 2.3 — Create `scripts/postgres-init.sql`

This runs automatically when the PostgreSQL container first starts. It creates the target schema and tables.

```sql
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
```

- [ ] `scripts/postgres-init.sql` created

---

## Step 2.4 — Create `monitoring/prometheus.yml`

```yaml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  - job_name: "kafka"
    static_configs:
      - targets: ["kafka:7071"]
    metrics_path: /metrics

  - job_name: "kafka-exporter"
    static_configs:
      - targets: ["kafka-exporter:9308"]

  - job_name: "kafka-connect"
    static_configs:
      - targets: ["kafka-connect:7072"]
    metrics_path: /metrics
```

- [ ] `monitoring/prometheus.yml` created

---

## Step 2.5 — Start all services

Run from inside the `kafka-pipeline/` directory:

- **Pull images one by one (more stable on slow connections)**

```
docker pull confluentinc/cp-kafka:7.6.1
docker pull confluentinc/cp-schema-registry:7.6.1
docker pull debezium/connect:2.6
docker pull provectuslabs/kafka-ui:latest
docker pull mysql:8.0
docker pull postgres:16
docker pull danielqsj/kafka-exporter:latest
docker pull prom/prometheus:latest
docker pull grafana/grafana:latest
```

- **Then run docker compose up -d after all pulls complete.**

```powershell
docker compose up -d
```

This pulls all images on first run (~15 minutes depending on internet speed). Subsequent starts take ~60 seconds.

Watch the startup progress:

```powershell
# Watch service health status (refresh every 5 seconds)
docker compose ps
```

Wait until all services show `healthy` or `running`. The `kafka-connect` container takes the longest (~2–3 minutes) because it waits for `kafka` and `schema-registry` to pass their healthchecks.

- [ ] `docker compose up -d` ran without errors

---

## Step 2.6 — Verify all services are healthy

```powershell
docker compose ps
```

Expected output (all services `running` or `healthy`):

```
NAME               IMAGE                                    STATUS
kafka              confluentinc/cp-kafka:7.6.1              Up (healthy)
schema-registry    confluentinc/cp-schema-registry:7.6.1   Up (healthy)
kafka-connect      debezium/connect:2.6                     Up (healthy)
kafka-ui           provectuslabs/kafka-ui:latest            Up
mysql-source       mysql:8.0                                Up
postgres-target    postgres:16                              Up
kafka-exporter     danielqsj/kafka-exporter:latest          Up
prometheus         prom/prometheus:latest                   Up
grafana            grafana/grafana:latest                   Up
```

Verify key endpoints respond:

```powershell
# Schema Registry
curl http://localhost:8081/subjects

# Kafka Connect (returns JSON with version info)
curl http://localhost:8083/

# Kafka UI
# Open http://localhost:8090 in your browser
```

- [ ] All containers are running
- [ ] `curl http://localhost:8081/subjects` returns `[]`
- [ ] `curl http://localhost:8083/` returns a JSON response
- [ ] http://localhost:8090 loads the Kafka UI

---

## Common Errors — Phase 2

**Error: `kafka-connect` keeps restarting**
- Cause: Kafka or Schema Registry not healthy yet.
- Fix: Wait 2–3 minutes and run `docker compose ps` again. Kafka Connect retries automatically.

**Error: `bind: address already in use` on port 3306, 5432, etc.**
- Cause: You have a local MySQL or PostgreSQL running on the same port.
- Fix: Stop the local service, or change the host port in `docker-compose.yml` (e.g., `"3307:3306"`).

**Error: Docker ran out of memory**
- Cause: Default Docker Desktop memory limit (often 2 GB) is too low.
- Fix: Docker Desktop → Settings → Resources → Memory → set to **8 GB**.

**Error: `CLUSTER_ID` mismatch on restart**
- Cause: The Kafka data volume has a different cluster ID from the one in `docker-compose.yml`.
- Fix: `docker compose down -v` (removes volumes, clean slate), then `docker compose up -d`.

---

---

# Phase 3: Kafka Topics (Day 1)

**Time estimate:** 10 minutes

---

## Step 3.1 — Create `scripts/create-topics.sh`

```bash
#!/bin/bash

KAFKA_CONTAINER="kafka"
BOOTSTRAP="localhost:9092"

create_topic() {
  local topic=$1
  local partitions=${2:-3}
  local replication=${3:-1}
  local retention_ms=${4:-604800000}  # 7 days default

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

# CDC source topics — one per table
create_topic "prod.mysql.sourcedb.orders"     3 1
create_topic "prod.mysql.sourcedb.customers"  3 1
create_topic "prod.mysql.sourcedb.products"   3 1

# Dead Letter Queue — retain forever (-1)
create_topic "prod.dlq.errors" 1 1 -1

# Kafka Connect internal topics (required before connectors can start)
create_topic "_connect-configs"  1 1
create_topic "_connect-offsets" 25 1
create_topic "_connect-status"   5 1

echo ""
echo "All topics created. Current topic list:"
docker exec $KAFKA_CONTAINER kafka-topics \
  --list \
  --bootstrap-server $BOOTSTRAP
```

- [ ] `scripts/create-topics.sh` created

---

## Step 3.2 — Run the topic creation script

```powershell
# Git Bash or WSL
bash scripts/create-topics.sh

# PowerShell alternative — run each command individually via docker exec
docker exec kafka kafka-topics --create --bootstrap-server localhost:9092 --topic prod.mysql.sourcedb.orders --partitions 3 --replication-factor 1 --config retention.ms=604800000 --if-not-exists

docker exec kafka kafka-topics --create --bootstrap-server localhost:9092 --topic prod.mysql.sourcedb.customers --partitions 3 --replication-factor 1 --config retention.ms=604800000 --if-not-exists

docker exec kafka kafka-topics --create --bootstrap-server localhost:9092 --topic prod.dlq.errors --partitions 1 --replication-factor 1 --config retention.ms=-1 --if-not-exists

docker exec kafka kafka-topics --create --bootstrap-server localhost:9092 --topic _connect-configs --partitions 1 --replication-factor 1 --if-not-exists

docker exec kafka kafka-topics --create --bootstrap-server localhost:9092 --topic _connect-offsets --partitions 25 --replication-factor 1 --if-not-exists

docker exec kafka kafka-topics --create --bootstrap-server localhost:9092 --topic _connect-status --partitions 5 --replication-factor 1 --if-not-exists
```

- [ ] Topics created without errors

---

## Step 3.3 — Verify topics in Kafka UI

1. Open http://localhost:8090
2. Click **Topics** in the left sidebar
3. You should see these topics listed:
   - `prod.mysql.sourcedb.orders` (3 partitions)
   - `prod.mysql.sourcedb.customers` (3 partitions)
   - `prod.mysql.sourcedb.products` (3 partitions)
   - `prod.dlq.errors` (1 partition)
   - `_connect-configs`, `_connect-offsets`, `_connect-status`

Alternatively, verify via CLI:

```powershell
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092
```

- [ ] All required topics appear in Kafka UI or CLI output

---

## Common Errors — Phase 3

**Error: `Error while executing topic command : Topic 'X' already exists`**
- This is safe to ignore. The `--if-not-exists` flag prevents it from being fatal.

**Error: `Connection to node -1 could not be established`**
- Kafka is not ready yet. Wait 30 seconds and retry.

**Error: Script not found or permission denied (Windows)**
- Use Git Bash or WSL to run `.sh` scripts, or use the PowerShell `docker exec` commands shown above.

---

---

# Phase 4: Source Connector — MySQL CDC (Day 2)

**Time estimate:** 15–20 minutes

---

## Step 4.1 — Create `connectors/source/mysql-cdc-source.json`

This configures the Debezium MySQL connector to read the MySQL binlog and publish change events to Kafka topics.

Key decisions in this config:
- Uses `kafka_user` (the same user created in `mysql-init.sql`) — no separate Debezium user needed for dev
- `snapshot.mode: initial` — takes a full snapshot of existing data on first start, then streams changes
- `ExtractNewRecordState` transform — unwraps the Debezium envelope so the Kafka message contains the flat row (not nested `before`/`after`)
- DLQ configured so bad records don't block the connector

```json
{
  "name": "mysql-cdc-source",
  "config": {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "tasks.max": "1",

    "database.hostname": "mysql",
    "database.port": "3306",
    "database.user": "kafka_user",
    "database.password": "kafka_password",
    "database.server.id": "184054",
    "database.server.name": "prod.mysql",

    "topic.prefix": "prod.mysql",
    "database.include.list": "sourcedb",
    "table.include.list": "sourcedb.orders,sourcedb.customers",

    "schema.history.internal.kafka.bootstrap.servers": "kafka:9092",
    "schema.history.internal.kafka.topic": "_schema-changes.mysql",

    "include.schema.changes": "true",
    "snapshot.mode": "initial",
    "snapshot.locking.mode": "minimal",

    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",

    "transforms": "unwrap,addMetadata",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.add.fields": "op,ts_ms,source.db,source.table",
    "transforms.unwrap.delete.handling.mode": "rewrite",
    "transforms.unwrap.drop.tombstones": "false",

    "transforms.addMetadata.type": "org.apache.kafka.connect.transforms.InsertField$Value",
    "transforms.addMetadata.static.field": "_pipeline_version",
    "transforms.addMetadata.static.value": "1.0",

    "errors.tolerance": "all",
    "errors.log.enable": "true",
    "errors.log.include.messages": "true",
    "errors.deadletterqueue.topic.name": "prod.dlq.errors",
    "errors.deadletterqueue.topic.replication.factor": "1",

    "heartbeat.interval.ms": "10000",
    "max.batch.size": "2048",
    "max.queue.size": "8192"
  }
}
```

- [ ] `connectors/source/mysql-cdc-source.json` created

---

## Step 4.2 — Register the source connector

```powershell
# Register via REST API
curl -X POST http://localhost:8083/connectors `
  -H "Content-Type: application/json" `
  -d "@connectors/source/mysql-cdc-source.json"
```

Git Bash version:

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @connectors/source/mysql-cdc-source.json
```

Expected response — the connector config echoed back as JSON with no error field.

- [ ] Connector registered without error

---

## Step 4.3 — Verify connector status

Wait 10–15 seconds for the connector to start, then:

```powershell
curl http://localhost:8083/connectors/mysql-cdc-source/status
```

Expected output:

```json
{
  "name": "mysql-cdc-source",
  "connector": {
    "state": "RUNNING",
    "worker_id": "kafka-connect:8083"
  },
  "tasks": [
    {
      "id": 0,
      "state": "RUNNING",
      "worker_id": "kafka-connect:8083"
    }
  ],
  "type": "source"
}
```

Both `connector.state` and `tasks[0].state` must be `RUNNING`.

- [ ] Connector state is `RUNNING`
- [ ] Task state is `RUNNING`

---

## Step 4.4 — Verify snapshot data appeared in Kafka

After the connector starts, Debezium immediately takes a snapshot of all existing rows and publishes them to Kafka.

```powershell
# Check message count in the orders topic
docker exec kafka kafka-run-class kafka.tools.GetOffsetShell `
  --broker-list localhost:9092 `
  --topic prod.mysql.sourcedb.orders
```

You should see non-zero offsets. The initial snapshot should have published the 2 sample orders.

In Kafka UI (http://localhost:8090):
1. Click **Topics** → `prod.mysql.sourcedb.orders`
2. Click **Messages** tab
3. You should see 2 messages from the initial snapshot

- [ ] Messages appear in `prod.mysql.sourcedb.orders` topic

---

## Common Errors — Phase 4

**Error: `connector.state` is `FAILED`**
- Run: `curl http://localhost:8083/connectors/mysql-cdc-source/status` and look at the `trace` field.
- Common sub-causes:
  - MySQL not ready: wait 30s and try again
  - Wrong credentials: verify `kafka_user`/`kafka_password` in the JSON
  - Binlog not enabled: check MySQL with `docker exec mysql-source mysql -u root -prootpassword -e "SHOW VARIABLES LIKE 'log_bin';"` — should show `ON`

**Error: `Access denied` for user `kafka_user`**
- The `mysql-init.sql` may not have run yet (MySQL takes ~30s to initialize on first start).
- Check: `docker logs mysql-source | tail -20`
- Fix: `docker compose restart mysql` and wait for it to be healthy again.

**Error: `_schema-changes.mysql` topic does not exist**
- Debezium creates this topic automatically. If it fails, manually create it:
  ```powershell
  docker exec kafka kafka-topics --create --bootstrap-server localhost:9092 --topic _schema-changes.mysql --partitions 1 --replication-factor 1
  ```

**Connector keeps restarting but never reaches RUNNING**
- Check the Kafka Connect logs: `docker logs kafka-connect --tail 50`
- The most common cause is a Schema Registry connection issue. Verify: `curl http://localhost:8081/subjects`

---

---

# Phase 5: Sink Connector — PostgreSQL (Day 2)

**Time estimate:** 15–20 minutes

---

## Step 5.1 — Create `connectors/sink/postgres-sink.json`

This configures the Confluent JDBC Sink Connector to read from the Kafka CDC topics and upsert records into PostgreSQL.

Key decisions:
- `insert.mode: upsert` with `pk.fields: id` — safe to replay; duplicate messages produce the same result
- `auto.create: true` — creates the table if it doesn't exist (relies on `table.name.format`)
- `table.name.format: pipeline.${topic}` — maps `prod.mysql.sourcedb.orders` → `pipeline.prod.mysql.sourcedb.orders`

> **Note on table naming:** The JDBC Sink uses the full topic name to construct the table name by default. Since the topic name contains dots, PostgreSQL will create a table named `pipeline."prod.mysql.sourcedb.orders"`. This works correctly — JDBC Sink quotes the table name. The target tables you created in `postgres-init.sql` (`pipeline.orders`, `pipeline.customers`) are separate and can be used with a custom Python consumer (Phase 6). For the sink connector, let it auto-create its own tables.

```json
{
  "name": "postgres-sink",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "tasks.max": "2",

    "connection.url": "jdbc:postgresql://postgres:5432/targetdb",
    "connection.user": "kafka_user",
    "connection.password": "kafka_password",

    "topics": "prod.mysql.sourcedb.orders,prod.mysql.sourcedb.customers",

    "insert.mode": "upsert",
    "pk.mode": "record_value",
    "pk.fields": "id",

    "auto.create": "true",
    "auto.evolve": "true",

    "table.name.format": "pipeline.${topic}",

    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",

    "transforms": "dropMetaFields",
    "transforms.dropMetaFields.type": "org.apache.kafka.connect.transforms.ReplaceField$Value",
    "transforms.dropMetaFields.exclude": "__op,__ts_ms,__source_db,__source_table,_pipeline_version",

    "batch.size": "3000",
    "max.retries": "10",
    "retry.backoff.ms": "3000",

    "errors.tolerance": "all",
    "errors.log.enable": "true",
    "errors.deadletterqueue.topic.name": "prod.dlq.errors",
    "errors.deadletterqueue.context.headers.enable": "true"
  }
}
```

- [ ] `connectors/sink/postgres-sink.json` created

---

## Step 5.2 — Register the sink connector

```powershell
curl -X POST http://localhost:8083/connectors `
  -H "Content-Type: application/json" `
  -d "@connectors/sink/postgres-sink.json"
```

Git Bash:

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @connectors/sink/postgres-sink.json
```

- [ ] Sink connector registered without error

---

## Step 5.3 — Verify sink connector status

```powershell
curl http://localhost:8083/connectors/postgres-sink/status
```

Expected: both `connector.state` and `tasks[*].state` are `RUNNING`.

- [ ] Sink connector state is `RUNNING`

---

## Step 5.4 — Verify data flowed MySQL → Kafka → PostgreSQL

Wait 30 seconds for the sink to process the initial snapshot messages, then:

```powershell
docker exec postgres-target psql -U kafka_user -d targetdb -c "SELECT COUNT(*) FROM pipeline.orders;"
```

Expected: `2` (the two seed rows from `mysql-init.sql`).

```powershell
docker exec postgres-target psql -U kafka_user -d targetdb -c "SELECT * FROM pipeline.customers;"
```

Expected: Alice Johnson and Bob Smith rows.

- [ ] Data from MySQL appears in PostgreSQL

---

## Step 5.5 — List all running connectors

```powershell
curl http://localhost:8083/connectors
```

Expected:

```json
["mysql-cdc-source","postgres-sink"]
```

- [ ] Both connectors listed

---

## Common Errors — Phase 5

**Error: `JdbcSinkConnector not found` or `connector.class not found`**
- Cause: The Debezium Connect image (`debezium/connect:2.6`) includes Debezium connectors but not the Confluent JDBC Sink.
- Fix: Install the JDBC Sink connector into the running container:
  ```powershell
  docker exec kafka-connect confluent-hub install confluentinc/kafka-connect-jdbc:10.7.4 --no-prompt
  docker restart kafka-connect
  ```
  Wait for kafka-connect to become healthy again before re-registering the sink.

**Error: `Table "pipeline.prod.mysql.sourcedb.orders" does not exist`**
- This is fine. `auto.create: true` will create it. Check PostgreSQL logs if it keeps failing:
  ```powershell
  docker logs postgres-target --tail 30
  ```

**Error: `Could not connect to postgres:5432`**
- Verify PostgreSQL container is running: `docker compose ps postgres`
- Verify connection from inside kafka-connect: `docker exec kafka-connect curl -s postgres:5432` (will return garbled bytes — that means port is reachable)

**Sink shows `RUNNING` but no data in PostgreSQL**
- Check consumer lag: `docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group connect-postgres-sink`
- If lag is 0, data was processed. If lag is non-zero, the sink is behind — wait.

---

---

# Phase 6: Python Producers/Consumers (Day 3)

**Time estimate:** 30–45 minutes

---

## Step 6.1 — Create CSV to Kafka producer: `scripts/csv_producer.py`

This producer reads a CSV file row-by-row and publishes each row to a Kafka topic using Avro serialization.

```python
# scripts/csv_producer.py
import csv
import json
import uuid
import sys
from pathlib import Path
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

KAFKA_BOOTSTRAP = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
TOPIC = "prod.files.csv.raw"

# Avro schema — adjust field names to match your CSV headers
AVRO_SCHEMA = """
{
  "type": "record",
  "name": "CsvRow",
  "namespace": "com.pipeline.files",
  "fields": [
    {"name": "id",           "type": ["null", "string"], "default": null},
    {"name": "name",         "type": ["null", "string"], "default": null},
    {"name": "amount",       "type": ["null", "string"], "default": null},
    {"name": "created_at",   "type": ["null", "string"], "default": null},
    {"name": "_source_file", "type": "string"},
    {"name": "_row_number",  "type": "int"}
  ]
}
"""

def delivery_report(err, msg):
    if err:
        print(f"Delivery failed for row {msg.key()}: {err}")
    # Uncomment for verbose success logging:
    # else:
    #     print(f"Delivered row to {msg.topic()}[{msg.partition()}] @ offset {msg.offset()}")

def produce_csv(file_path: str):
    sr_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    serializer = AvroSerializer(sr_client, AVRO_SCHEMA)
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

    file_name = Path(file_path).name
    row_num = 0

    with open(file_path, newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row_num, row in enumerate(reader, start=1):
            # Build record — only include fields that exist in the schema
            record = {
                "id":           row.get("id"),
                "name":         row.get("name"),
                "amount":       row.get("amount"),
                "created_at":   row.get("created_at"),
                "_source_file": file_name,
                "_row_number":  row_num,
            }

            producer.produce(
                topic=TOPIC,
                key=str(uuid.uuid4()),
                value=serializer(record, SerializationContext(TOPIC, MessageField.VALUE)),
                on_delivery=delivery_report
            )

            # Poll to trigger delivery callbacks and prevent buffer overflow
            if row_num % 1000 == 0:
                producer.poll(0)
                print(f"Produced {row_num} rows...")

    producer.flush()
    print(f"Finished. Produced {row_num} rows from {file_name} to topic {TOPIC}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python scripts/csv_producer.py <path/to/file.csv>")
        sys.exit(1)
    produce_csv(sys.argv[1])
```

- [ ] `scripts/csv_producer.py` created

---

## Step 6.2 — Create a sample CSV to test the producer

Create `scripts/sample_data.csv`:

```csv
id,name,amount,created_at
1,Widget A,19.99,2024-01-15 10:00:00
2,Widget B,34.50,2024-01-15 10:05:00
3,Widget C,9.99,2024-01-15 10:10:00
```

- [ ] Sample CSV created

---

## Step 6.3 — Create the CSV topic and run the producer

First, create the topic for CSV data:

```powershell
docker exec kafka kafka-topics --create `
  --bootstrap-server localhost:9092 `
  --topic prod.files.csv.raw `
  --partitions 3 `
  --replication-factor 1 `
  --if-not-exists
```

Then run the producer (make sure your venv is activated):

```powershell
python scripts/csv_producer.py scripts/sample_data.csv
```

Expected output:
```
Finished. Produced 3 rows from sample_data.csv to topic prod.files.csv.raw
```

- [ ] Producer ran without errors
- [ ] Messages appear in Kafka UI under `prod.files.csv.raw`

---

## Step 6.4 — Create custom Avro consumer: `consumers/postgres_consumer.py`

This consumer reads from the CDC orders topic and writes to PostgreSQL with UPSERT logic. It handles inserts, updates, and deletes correctly.

First, create the `consumers/` directory:

```powershell
mkdir consumers
```

Then create `consumers/postgres_consumer.py`:

```python
# consumers/postgres_consumer.py
import psycopg2
import psycopg2.extras
import logging
import signal
import sys
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
logger = logging.getLogger(__name__)

KAFKA_CONFIG = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "custom-postgres-consumer",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
    "max.poll.interval.ms": 300000,
    "session.timeout.ms": 30000,
}

SCHEMA_REGISTRY_URL = "http://localhost:8081"
TOPICS = ["prod.mysql.sourcedb.orders"]
BATCH_SIZE = 500

PG_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "targetdb",
    "user": "kafka_user",
    "password": "kafka_password",
}

UPSERT_SQL = """
INSERT INTO pipeline.orders
    (id, customer_id, product_id, quantity, amount, status, created_at, updated_at, _cdc_op, _ingested_at)
VALUES
    (%(id)s, %(customer_id)s, %(product_id)s, %(quantity)s, %(amount)s,
     %(status)s, %(created_at)s, %(updated_at)s, %(__op)s, NOW())
ON CONFLICT (id) DO UPDATE SET
    customer_id  = EXCLUDED.customer_id,
    product_id   = EXCLUDED.product_id,
    quantity     = EXCLUDED.quantity,
    amount       = EXCLUDED.amount,
    status       = EXCLUDED.status,
    updated_at   = EXCLUDED.updated_at,
    _cdc_op      = EXCLUDED._cdc_op,
    _ingested_at = NOW();
"""

DELETE_SQL = "DELETE FROM pipeline.orders WHERE id = %(id)s"

running = True

def signal_handler(sig, frame):
    global running
    logger.info("Shutdown signal received — stopping after current batch")
    running = False

def process_batch(conn, batch):
    upserts = [r for r in batch if r.get("__op") != "d"]
    deletes = [r for r in batch if r.get("__op") == "d"]

    with conn.cursor() as cur:
        if upserts:
            psycopg2.extras.execute_batch(cur, UPSERT_SQL, upserts, page_size=200)
        if deletes:
            psycopg2.extras.execute_batch(cur, DELETE_SQL, deletes, page_size=200)

    conn.commit()
    logger.info(f"Processed batch: {len(upserts)} upserts, {len(deletes)} deletes")

def main():
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    sr_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    deserializer = AvroDeserializer(sr_client)
    consumer = Consumer(KAFKA_CONFIG)
    consumer.subscribe(TOPICS)
    conn = psycopg2.connect(**PG_CONFIG)

    batch = []
    offsets_to_commit = []

    try:
        while running:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                if batch:
                    process_batch(conn, batch)
                    consumer.commit(offsets=offsets_to_commit)
                    batch.clear()
                    offsets_to_commit.clear()
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error(f"Consumer error: {msg.error()}")
                continue

            value = deserializer(
                msg.value(),
                SerializationContext(msg.topic(), MessageField.VALUE)
            )

            if value:
                batch.append(value)
                offsets_to_commit.append(msg)

            if len(batch) >= BATCH_SIZE:
                process_batch(conn, batch)
                consumer.commit(offsets=offsets_to_commit)
                batch.clear()
                offsets_to_commit.clear()

    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        conn.rollback()
        sys.exit(1)
    finally:
        if batch:
            try:
                process_batch(conn, batch)
                consumer.commit(offsets=offsets_to_commit)
            except Exception as e:
                logger.error(f"Error flushing final batch: {e}")
        consumer.close()
        conn.close()
        logger.info("Consumer shut down cleanly.")

if __name__ == "__main__":
    main()
```

- [ ] `consumers/postgres_consumer.py` created

---

## Step 6.5 — Run the custom consumer

Make sure your venv is activated, then:

```powershell
python consumers/postgres_consumer.py
```

Expected output (it will keep running until you press Ctrl+C):

```
2024-01-15 10:00:00 INFO Processed batch: 2 upserts, 0 deletes
```

Stop it with `Ctrl+C` when done testing.

- [ ] Consumer runs and processes messages
- [ ] Data appears in `pipeline.orders` table

---

## Common Errors — Phase 6

**Error: `ModuleNotFoundError: No module named 'confluent_kafka'`**
- Your venv is not activated. Run `.\.venv\Scripts\Activate.ps1` first.

**Error: `Schema Registry connection refused`**
- Verify Schema Registry is running: `curl http://localhost:8081/subjects`
- The producer connects to `localhost:8081` (host machine), which maps to the Schema Registry container.

**Error: `SSL: CERTIFICATE_VERIFY_FAILED` on Windows**
- Not expected in this dev setup (no TLS configured). If it appears, check if you have a proxy intercepting traffic.

**CSV producer fails with `KeyError: 'id'`**
- Your CSV file headers don't match the Avro schema field names. Edit `AVRO_SCHEMA` in `csv_producer.py` to match your actual CSV columns.

---

---

# Phase 7: Monitoring (Day 4)

**Time estimate:** 20–30 minutes

---

## Step 7.1 — Verify Prometheus is collecting metrics

Open http://localhost:9090 in your browser.

1. Click **Status** → **Targets**
2. You should see these scrape targets:
   - `kafka` at `kafka:7071` — may show as DOWN initially (JMX not configured yet — see note below)
   - `kafka-exporter` at `kafka-exporter:9308` — should be UP
   - `kafka-connect` at `kafka-connect:7072` — may show as DOWN

> **Note:** The `kafka:7071` JMX exporter requires additional JMX exporter agent configuration on the Kafka container to serve metrics in Prometheus format. For this dev setup, the most useful metrics come from `kafka-exporter` (consumer lag, topic metrics). JMX exporter setup is covered in `09-monitoring.md` for production deployments.

The `kafka-exporter` target should be UP and collecting:

```promql
# Test in Prometheus query box
kafka_consumergroup_lag
```

- [ ] Prometheus is accessible at http://localhost:9090
- [ ] `kafka-exporter` target is UP

---

## Step 7.2 — Access Grafana

Open http://localhost:3000

Login credentials:
- Username: `admin`
- Password: `admin`

You will be prompted to change the password on first login — you can skip this for dev.

- [ ] Grafana accessible at http://localhost:3000

---

## Step 7.3 — Add Prometheus as a data source in Grafana

1. In Grafana, click the hamburger menu (top left) → **Connections** → **Data sources**
2. Click **Add data source**
3. Select **Prometheus**
4. Set the URL to: `http://prometheus:9090`
   - Use `prometheus` (the Docker container hostname), not `localhost`
5. Click **Save & test**
6. You should see "Data source is working"

- [ ] Prometheus data source added to Grafana

---

## Step 7.4 — Import a Kafka dashboard

1. In Grafana, click the **+** icon → **Import**
2. Enter Dashboard ID: **7589** (Kafka Exporter Overview)
3. Click **Load**
4. Select the Prometheus data source you just added
5. Click **Import**

This dashboard shows:
- Message in/out rates per topic
- Consumer group lag
- Active consumer group count
- Topic partition count

Useful additional dashboard IDs:
- `11173` — Kafka Connect metrics
- `14012` — Kafka Lag Exporter
- `8563` — JVM Overview

- [ ] Dashboard 7589 imported and showing data

---

## Step 7.5 — Key metrics to watch

Once the dashboard loads, look for these indicators:

**Consumer Lag (most important)**
```promql
sum by (consumergroup, topic) (kafka_consumergroup_lag)
```
- Should be 0 or near-0 when the pipeline is idle
- Increases during high insert load, then returns to 0

**Messages produced per second**
```promql
rate(kafka_topic_partition_current_offset[1m])
```

**Consumer group health**
```promql
kafka_consumergroup_members
```
- `connect-mysql-cdc-source` should have 1 member
- `connect-postgres-sink` should have members equal to `tasks.max` (2)

**Connector task failures (via Connect REST API)**
```powershell
curl http://localhost:8083/connectors/mysql-cdc-source/status
curl http://localhost:8083/connectors/postgres-sink/status
```

- [ ] Consumer lag metric is visible in Grafana

---

## Common Errors — Phase 7

**Grafana shows "No data" on all panels**
- Verify the data source URL is `http://prometheus:9090` (not `localhost:9090`)
- Click the data source and run "Save & test" again

**Kafka exporter shows as DOWN in Prometheus targets**
- Verify it's running: `docker compose ps kafka-exporter`
- Check logs: `docker logs kafka-exporter --tail 20`
- It may fail if Kafka isn't ready. Restart it: `docker compose restart kafka-exporter`

**Prometheus target `kafka:7071` is always DOWN**
- This requires the JMX Prometheus exporter agent to be configured inside the Kafka container. This is an advanced setup. For basic monitoring, `kafka-exporter:9308` is sufficient.

---

---

# Phase 8: Testing the Full Pipeline (Day 4)

**Time estimate:** 30–45 minutes

---

## Step 8.1 — Insert test data into MySQL and watch it appear in Kafka

Open two terminal windows side by side.

**Terminal 1** — watch Kafka topic messages in real-time:

```powershell
docker exec kafka kafka-console-consumer `
  --bootstrap-server localhost:9092 `
  --topic prod.mysql.sourcedb.orders `
  --from-beginning `
  --property print.headers=true
```

**Terminal 2** — insert test data into MySQL:

```powershell
docker exec mysql-source mysql -u kafka_user -pkafka_password sourcedb -e "
INSERT INTO orders (customer_id, product_id, quantity, amount, status)
VALUES (1, 201, 3, 149.99, 'PENDING');
"
```

Expected: Within 1–2 seconds, Terminal 1 should print a new Avro-encoded message (will appear as garbled binary — that is expected for Avro). Use Kafka UI for readable output.

**In Kafka UI:**
1. Open http://localhost:8090 → **Topics** → `prod.mysql.sourcedb.orders`
2. Click **Messages** tab
3. You should see the new message with `__op: "c"` (create)

- [ ] New MySQL insert appears in Kafka topic within 2 seconds

---

## Step 8.2 — Verify it lands in PostgreSQL

After the insert, wait 3–5 seconds, then:

```powershell
docker exec postgres-target psql -U kafka_user -d targetdb -c "
SELECT id, customer_id, amount, status, _ingested_at
FROM pipeline.orders
ORDER BY id DESC
LIMIT 5;
"
```

You should see the new row with `status = 'PENDING'`.

- [ ] New order appears in PostgreSQL

---

## Step 8.3 — Test an UPDATE

```powershell
docker exec mysql-source mysql -u kafka_user -pkafka_password sourcedb -e "
UPDATE orders SET status = 'PROCESSING' WHERE customer_id = 1 AND status = 'PENDING';
"
```

Wait 3 seconds, then verify in PostgreSQL:

```powershell
docker exec postgres-target psql -U kafka_user -d targetdb -c "
SELECT id, status FROM pipeline.orders WHERE status = 'PROCESSING';
"
```

In Kafka UI, the new message should show `__op: "u"` (update).

- [ ] UPDATE propagates from MySQL to PostgreSQL

---

## Step 8.4 — Test a DELETE

```powershell
# Get the ID of the order we just inserted
docker exec mysql-source mysql -u kafka_user -pkafka_password sourcedb -e "
SELECT id FROM orders ORDER BY id DESC LIMIT 1;
"

# Delete it (replace <ID> with the actual ID)
docker exec mysql-source mysql -u kafka_user -pkafka_password sourcedb -e "
DELETE FROM orders WHERE id = <ID>;
"
```

Wait 3 seconds, then verify:

```powershell
docker exec postgres-target psql -U kafka_user -d targetdb -c "
SELECT COUNT(*) FROM pipeline.orders WHERE id = <ID>;
"
```

Expected: `0`

> **Note:** For DELETE to work, the JDBC Sink Connector must be configured with `delete.enabled: true` and tombstone handling. The current `postgres-sink.json` uses upsert mode only. To enable deletes, see `06-data-sink.md` "With Delete Support" section. For this test, the `__op` field is set to `"d"` — you can verify with the custom consumer in Phase 6 which handles deletes explicitly.

- [ ] DELETE message appears in Kafka with `__op: "d"`

---

## Step 8.5 — Test failure and recovery (DLQ scenario)

**Test 1: Sink failure and Kafka durability**

```powershell
# 1. Stop PostgreSQL
docker compose stop postgres

# 2. Insert data into MySQL while PG is down
docker exec mysql-source mysql -u kafka_user -pkafka_password sourcedb -e "
INSERT INTO orders (customer_id, product_id, quantity, amount, status)
VALUES (2, 888, 5, 499.99, 'RECOVERY_TEST');
"

# 3. Data is safely stored in Kafka
echo "Data is in Kafka. PostgreSQL is down."

# 4. Restart PostgreSQL
docker compose start postgres

# 5. Wait for sink connector to auto-recover (~30s)
# Watch connector status
curl http://localhost:8083/connectors/postgres-sink/status

# 6. Verify data appeared after recovery
docker exec postgres-target psql -U kafka_user -d targetdb -c "
SELECT * FROM pipeline.orders WHERE amount = 499.99;
"
```

Expected: The row with `amount = 499.99` appears in PostgreSQL after PostgreSQL comes back up. This demonstrates **Kafka as a durable buffer** — no data loss even when the sink is temporarily unavailable.

- [ ] Data written to MySQL while PG is down eventually lands in PostgreSQL after recovery

---

## Step 8.6 — Test source failure recovery

```powershell
# 1. Stop MySQL
docker compose stop mysql

# 2. Watch connector status change (wait ~30s for FAILED)
curl http://localhost:8083/connectors/mysql-cdc-source/status

# 3. Restart MySQL
docker compose start mysql

# 4. Wait for Debezium to auto-reconnect (~30s)
# Check connector status
curl http://localhost:8083/connectors/mysql-cdc-source/status

# 5. If still FAILED, restart the connector manually
curl -X POST http://localhost:8083/connectors/mysql-cdc-source/restart

# 6. Insert a record to confirm streaming resumed
docker exec mysql-source mysql -u kafka_user -pkafka_password sourcedb -e "
INSERT INTO orders (customer_id, product_id, quantity, amount, status)
VALUES (1, 999, 1, 299.99, 'POST_RECOVERY');
"

# 7. Verify it appears in PostgreSQL
docker exec postgres-target psql -U kafka_user -d targetdb -c "
SELECT * FROM pipeline.orders WHERE status = 'POST_RECOVERY';
"
```

- [ ] CDC resumes after MySQL restart

---

## Step 8.7 — Check the DLQ topic

After the failure tests, check if anything ended up in the DLQ:

```powershell
docker exec kafka kafka-console-consumer `
  --bootstrap-server localhost:9092 `
  --topic prod.dlq.errors `
  --from-beginning `
  --max-messages 10 `
  --timeout-ms 5000
```

In a healthy pipeline with valid data, the DLQ should be empty. Any messages here indicate records that failed processing and need investigation.

- [ ] DLQ is empty (or any messages are understood and handled)

---

## Step 8.8 — Watch consumer lag during load

```powershell
# Monitor consumer lag in real-time
docker exec kafka kafka-consumer-groups `
  --bootstrap-server localhost:9092 `
  --describe `
  --group connect-postgres-sink
```

The `LAG` column shows how many messages the sink is behind. During normal operation it should be 0. During bulk inserts it will increase and then drain back to 0.

- [ ] Consumer lag returns to 0 after bulk insert

---

## Step 8.9 — Run a load test (optional)

Create `scripts/load_test.py`:

```python
# scripts/load_test.py
"""
Inserts 1000 orders into MySQL in batches of 100
and measures pipeline end-to-end latency.
"""
import mysql.connector
import psycopg2
import time
import random

MYSQL_CONFIG = {
    "host": "localhost", "port": 3307,
    "database": "sourcedb",
    "user": "kafka_user", "password": "kafka_password"
}
PG_CONFIG = {
    "host": "localhost", "port": 5432,
    "dbname": "targetdb",
    "user": "kafka_user", "password": "kafka_password"
}

NUM_ORDERS = 1000
BATCH_SIZE = 100

def run_load_test():
    mysql_conn = mysql.connector.connect(**MYSQL_CONFIG)
    pg_conn = psycopg2.connect(**PG_CONFIG)
    mysql_cur = mysql_conn.cursor()
    pg_cur = pg_conn.cursor()

    print(f"Inserting {NUM_ORDERS} orders into MySQL in batches of {BATCH_SIZE}...")
    start_time = time.time()

    for batch_start in range(0, NUM_ORDERS, BATCH_SIZE):
        batch = [
            (random.randint(1, 2), random.randint(100, 999),
             random.randint(1, 10), round(random.uniform(10, 500), 2),
             random.choice(['PENDING', 'PROCESSING', 'COMPLETED']))
            for _ in range(BATCH_SIZE)
        ]
        mysql_cur.executemany(
            "INSERT INTO orders (customer_id, product_id, quantity, amount, status) VALUES (%s,%s,%s,%s,%s)",
            batch
        )
        mysql_conn.commit()
        print(f"  Inserted {batch_start + BATCH_SIZE}/{NUM_ORDERS} orders...")

    insert_done_time = time.time()
    print(f"MySQL inserts done in {insert_done_time - start_time:.2f}s")

    # Get the max ID that was inserted
    mysql_cur.execute("SELECT MAX(id) FROM orders")
    max_id = mysql_cur.fetchone()[0]
    print(f"Waiting for PostgreSQL to catch up (max_id={max_id})...")

    wait_start = time.time()
    while True:
        pg_cur.execute("SELECT MAX(id) FROM pipeline.orders")
        pg_max = pg_cur.fetchone()[0] or 0
        if pg_max >= max_id:
            break
        elapsed = time.time() - wait_start
        if elapsed > 120:
            print("Timeout waiting for sync!")
            break
        print(f"  PG max_id={pg_max}, waiting... ({elapsed:.1f}s)")
        time.sleep(1)

    total_time = time.time() - start_time
    sync_latency = time.time() - insert_done_time
    print(f"\nResults:")
    print(f"  Total time (insert + sync): {total_time:.2f}s")
    print(f"  Pipeline sync latency: {sync_latency:.2f}s")
    print(f"  Throughput: {NUM_ORDERS / total_time:.0f} events/sec end-to-end")

    mysql_conn.close()
    pg_conn.close()

if __name__ == "__main__":
    run_load_test()
```

Run it:

```powershell
python scripts/load_test.py
```

Expected results on a typical dev machine:
- Insert rate: ~500–2000 rows/sec into MySQL
- Pipeline sync latency: 2–10 seconds for 1000 rows
- End-to-end throughput: 100–500 events/sec (single-node dev)

- [ ] Load test completes with sync latency under 30 seconds

---

---

# Quick Reference: Common Commands

## Service Management

```powershell
# Start all services
docker compose up -d

# Stop all services (keeps data volumes)
docker compose down

# Stop and DELETE all data (clean slate)
docker compose down -v

# View logs for a specific service
docker compose logs -f kafka-connect

# Restart a single service
docker compose restart kafka-connect

# Check service health
docker compose ps
```

## Connector Management

```powershell
# List all connectors
curl http://localhost:8083/connectors

# Check connector status
curl http://localhost:8083/connectors/mysql-cdc-source/status
curl http://localhost:8083/connectors/postgres-sink/status

# Restart a connector (after fixing a config error)
curl -X POST http://localhost:8083/connectors/mysql-cdc-source/restart

# Pause a connector
curl -X PUT http://localhost:8083/connectors/postgres-sink/pause

# Resume a connector
curl -X PUT http://localhost:8083/connectors/postgres-sink/resume

# Delete a connector
curl -X DELETE http://localhost:8083/connectors/postgres-sink

# Update connector config
curl -X PUT http://localhost:8083/connectors/postgres-sink/config `
  -H "Content-Type: application/json" `
  -d "{""batch.size"": ""5000""}"
```

## Kafka CLI

```powershell
# List topics
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Describe a topic (partitions, replication, config)
docker exec kafka kafka-topics --describe `
  --topic prod.mysql.sourcedb.orders `
  --bootstrap-server localhost:9092

# Check consumer group lag
docker exec kafka kafka-consumer-groups `
  --bootstrap-server localhost:9092 `
  --describe `
  --group connect-postgres-sink

# Consume messages from beginning (for debugging)
docker exec kafka kafka-console-consumer `
  --bootstrap-server localhost:9092 `
  --topic prod.mysql.sourcedb.orders `
  --from-beginning `
  --max-messages 10

# List all consumer groups
docker exec kafka kafka-consumer-groups `
  --bootstrap-server localhost:9092 `
  --list
```

## Database Quick Access

```powershell
# Connect to MySQL
docker exec -it mysql-source mysql -u kafka_user -pkafka_password sourcedb

# Connect to PostgreSQL
docker exec -it postgres-target psql -U kafka_user -d targetdb

# Quick query without interactive shell
docker exec mysql-source mysql -u kafka_user -pkafka_password sourcedb -e "SELECT COUNT(*) FROM orders;"
docker exec postgres-target psql -U kafka_user -d targetdb -c "SELECT COUNT(*) FROM pipeline.orders;"
```

## Schema Registry

```powershell
# List all registered schemas
curl http://localhost:8081/subjects

# Get schema versions for a topic
curl http://localhost:8081/subjects/prod.mysql.sourcedb.orders-value/versions

# Get latest schema
curl http://localhost:8081/subjects/prod.mysql.sourcedb.orders-value/versions/latest
```

---

---

# Final Checklist — Complete Pipeline

After completing all 8 phases, verify the full end-to-end pipeline:

## Infrastructure
- [ ] All 9 Docker containers running and healthy
- [ ] Kafka UI accessible at http://localhost:8090
- [ ] Schema Registry accessible at http://localhost:8081
- [ ] Kafka Connect REST API accessible at http://localhost:8083
- [ ] Prometheus accessible at http://localhost:9090
- [ ] Grafana accessible at http://localhost:3000

## Topics
- [ ] `prod.mysql.sourcedb.orders` exists (3 partitions)
- [ ] `prod.mysql.sourcedb.customers` exists (3 partitions)
- [ ] `prod.dlq.errors` exists (1 partition, infinite retention)
- [ ] `_connect-configs`, `_connect-offsets`, `_connect-status` exist

## Connectors
- [ ] `mysql-cdc-source` connector is RUNNING
- [ ] `postgres-sink` connector is RUNNING
- [ ] Both connectors show 0 failed tasks

## Data Flow
- [ ] MySQL seed data (2 orders, 2 customers) visible in Kafka topics
- [ ] MySQL seed data visible in PostgreSQL target tables
- [ ] New MySQL INSERT appears in Kafka within 2 seconds
- [ ] New MySQL INSERT appears in PostgreSQL within 5 seconds
- [ ] MySQL UPDATE propagates to PostgreSQL
- [ ] Pipeline recovers automatically after PostgreSQL restart

## Monitoring
- [ ] Prometheus is scraping `kafka-exporter` successfully
- [ ] Grafana dashboard 7589 shows topic and consumer group metrics
- [ ] Consumer lag is 0 during idle periods

---

---

# Troubleshooting Index

| Symptom | Phase | Fix |
|---------|-------|-----|
| kafka-connect keeps restarting | 2 | Wait 2–3 min; Kafka/SR not ready |
| Port already in use | 2 | Stop local service or change port in docker-compose.yml |
| Out of memory errors | 2 | Increase Docker Desktop memory to 8 GB |
| `CLUSTER_ID` mismatch | 2 | `docker compose down -v` then restart |
| Connector state FAILED | 4/5 | Check `docker logs kafka-connect`; check credentials |
| `JdbcSinkConnector not found` | 5 | Install via `confluent-hub install` inside container |
| No data in PostgreSQL | 5 | Check consumer lag; verify connector RUNNING |
| ModuleNotFoundError | 6 | Activate venv: `.\.venv\Scripts\Activate.ps1` |
| Consumer lag never returns to 0 | 8 | Increase `tasks.max` in sink connector config |
| DLQ has messages | 8 | Inspect with `kafka-console-consumer` to find root cause |

---

# Reference Documents

All detailed documentation is in the `Documents/` directory:

| File | Contents |
|------|----------|
| `01-architecture.md` | System architecture diagrams and component breakdown |
| `02-technology-stack.md` | Technology choices, sizing guides, connector ecosystem |
| `03-environment-setup.md` | Docker Compose reference, KRaft config, CLI commands |
| `04-data-ingestion.md` | Debezium CDC configs (MySQL, PostgreSQL, Oracle), CSV producer |
| `05-data-processing.md` | Kafka Streams, ksqlDB, Flink examples |
| `06-data-sink.md` | JDBC Sink configs (PostgreSQL, MySQL, SQL Server), custom consumer |
| `07-schema-management.md` | Avro/Protobuf/JSON schema examples, Schema Registry API |
| `08-error-handling.md` | DLQ, retry strategies, exactly-once, idempotent sinks |
| `09-monitoring.md` | Prometheus queries, alert rules, Grafana dashboards |
| `10-security.md` | TLS, SASL/SCRAM, ACLs, secrets management |
| `11-scaling-performance.md` | Partition tuning, broker/producer/consumer config |
| `12-deployment.md` | Kubernetes (Strimzi/Helm), CI/CD pipeline |
| `13-real-world-example.md` | End-to-end walkthrough with load test and failure scenarios |
