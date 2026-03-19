# Kafka Pipeline — Complete Implementation Guide

---

## Fresh Start Checklist

Use this before every new setup. All boxes must be checked before moving to Phase 4.

### Files that must exist before `docker compose up -d`

- [ ] `docker-compose.yml`
- [ ] `scripts/mysql-init.sql`
- [ ] `scripts/postgres-init.sql`
- [ ] `monitoring/prometheus.yml`
- [ ] `connectors/source/mysql-cdc-source.json`
- [ ] `connectors/sink/postgres-sink.json`

> **Why:** `docker compose up -d` mounts `monitoring/prometheus.yml` at startup. If the file is missing, Prometheus container fails. MySQL and PostgreSQL init scripts also run at first container start only — if they're missing, tables won't be created.

### Connector JSON files must use Option A values for Docker setup

| File | `hostname` should be | `password` should be |
|------|---------------------|---------------------|
| `mysql-cdc-source.json` | `"mysql"` | `"kafka_password"` |
| `postgres-sink.json` | `"postgres"` in URL | `"kafka_password"` |

> If you previously used VPS (Option B) credentials, reset them to Option A values before starting fresh with Docker.

### Topics must be created BEFORE registering connectors

Kafka Connect stores connector state in Kafka topics. If `_connect-configs`, `_connect-offsets`, `_connect-status` don't exist, connectors fail silently.

The following topics must all exist:
- `prod.mysql.sourcedb.orders`
- `prod.mysql.sourcedb.customers`
- `prod.dlq.errors`
- `_connect-configs`
- `_connect-offsets`
- `_connect-status`
- `__debezium-heartbeat.prod.mysql`
- `prod.mysql`

### Order of operations (do NOT skip steps)

```
1. docker compose up -d
2. Wait for kafka-connect → healthy  (~2-3 min)
3. Create all Kafka topics            (Phase 5)
4. Register source connector          (Phase 6)
5. Wait for snapshot to complete      (~30 sec)
6. Register sink connector            (Phase 7)
7. Verify data in PostgreSQL
```

---

**For:** A developer starting from scratch who wants to build a production-grade MySQL → Kafka → PostgreSQL CDC pipeline.

**Environments covered:** Windows 11 (PowerShell), Linux AlmaLinux 9, macOS Apple Silicon (M1/M2/M3)

**Database options:**
- **Option A — Docker:** MySQL and PostgreSQL run as Docker containers (best for local dev and learning)
- **Option B — VPS/External:** MySQL and PostgreSQL run on a remote Linux server (production-like)

**Stack versions (tested and working):**
- Apache Kafka `7.6.1` (KRaft mode — no Zookeeper needed)
- Debezium Connect `2.6` (includes both MySQL source + JDBC sink connectors)
- Schema Registry `7.6.1`
- Kafka UI (provectuslabs) — latest
- Python 3.13

---

## What This Pipeline Does

```
MySQL (source database)
    │
    │  Reads binlog (Change Data Capture)
    ▼
Debezium MySQL Source Connector  ← runs inside Kafka Connect
    │
    │  Publishes JSON messages with schema
    ▼
Apache Kafka  (topics: prod.mysql.sourcedb.orders, prod.mysql.sourcedb.customers)
    │
    │  Reads messages
    ▼
Debezium JDBC Sink Connector  ← runs inside Kafka Connect
    │
    │  JDBC UPSERT
    ▼
PostgreSQL (target database)

Monitoring: Prometheus + Grafana + Kafka Exporter + Kafka UI
```

**What CDC means:** Instead of running periodic queries (`SELECT * WHERE updated_at > last_run`), Debezium reads MySQL's binary log (binlog) in real-time. Every INSERT, UPDATE, and DELETE in MySQL is captured as an event and published to Kafka within milliseconds. This is how banking systems, e-commerce platforms, and analytics pipelines work at scale.

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

> **Why 8090 for Kafka UI?** Port 8080 is commonly used by local development servers (Tomcat, Spring Boot, etc.). Using 8090 avoids conflicts.
>
> **Why 3307 for MySQL?** Port 3306 may already be used by a local MySQL installation on Windows. Using 3307 as the host port avoids the `bind: address already in use` error.

---

## System Requirements

| Resource | Minimum | Recommended |
|----------|---------|-------------|
| RAM | 6 GB free for Docker | 8 GB |
| Disk | 8 GB free | 15 GB |
| Docker Desktop | 29.x | 29.2.1+ |
| Docker Compose | v5.x | v5.1.0+ |
| Python | 3.11+ | 3.13 |

---

---

# Phase 1: System Prerequisites

---

## Step 1.1 — Install Docker Desktop

**Why:** All Kafka infrastructure (broker, connect, schema registry, monitoring) runs in Docker containers. Docker Desktop provides the container runtime on Windows and Mac.

**Windows / Mac:**
- Download from https://www.docker.com/products/docker-desktop/
- Install and launch it
- Go to **Settings → Resources → Memory** and set to **8 GB minimum**

**Linux (AlmaLinux 9):**

```bash
sudo dnf install -y yum-utils
sudo yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo
sudo dnf install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin
sudo systemctl enable --now docker
sudo usermod -aG docker $USER
newgrp docker
```

Verify:

```bash
docker --version         # Docker version 29.x.x
docker compose version   # Docker Compose version v5.x.x
```

---

## Step 1.2 — Create Python virtual environment

**Why:** Python dependencies (kafka client, psycopg2, mysql connector) should be isolated in a venv so they don't conflict with other projects on your machine.

**Windows PowerShell:**

```powershell
# Navigate to your project folder first
cd D:\Personal\Project\KafkaPipeline

# Create venv
python -m venv venv

# Activate it
.\venv\Scripts\Activate.ps1

# If you get a script execution policy error:
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
.\venv\Scripts\Activate.ps1
```

**Linux (AlmaLinux 9):**

```bash
cd ~/kafka-pipeline
python3 -m venv venv
source venv/bin/activate
```

**macOS (M1/M2/M3):**

```bash
cd ~/kafka-pipeline
python3 -m venv venv
source venv/bin/activate
```

Your prompt should now show `(venv)` at the start.

Verify Python:

```bash
python --version    # should show 3.11+ or 3.13
```

---

## Step 1.3 — Install Python dependencies

**Why:** `confluent-kafka` is the official Kafka client library for Python. `psycopg2-binary` lets Python connect to PostgreSQL. `mysql-connector-python` lets Python connect to MySQL.

**All platforms (run with venv activated):**

```bash
pip install confluent-kafka psycopg2-binary mysql-connector-python
pip freeze > requirements.txt
```

Verify:

```bash
python -c "import confluent_kafka; print('confluent_kafka OK')"
python -c "import psycopg2; print('psycopg2 OK')"
python -c "import mysql.connector; print('mysql.connector OK')"
```

---

---

# Phase 2: Project Folder Structure

---

## Step 2.1 — Create the folder layout

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

**Linux / macOS:**

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

---

# Phase 3: VPS Pre-requisites (Option B Only)

> **Skip this entire phase if you are using Option A (Docker databases).**
> Only read this if you want MySQL and/or PostgreSQL to run on a remote VPS.

---

## Why VPS requires pre-configuration

Docker MySQL comes pre-configured with binlog enabled (via the `--log-bin` command in docker-compose). A VPS MySQL installed via `apt` or `dnf` does NOT have binlog enabled by default. Similarly, VPS PostgreSQL needs `wal_level = logical` for replication. These must be set before Debezium can capture changes.

---

## Step 3.1 — Configure VPS MySQL for CDC

SSH into your VPS:

```bash
ssh root@62.171.177.208
```

**Check if binlog is already enabled:**

```bash
mysql -u root -p -e "SHOW VARIABLES LIKE 'log_bin'; SHOW VARIABLES LIKE 'binlog_format';"
```

Expected: `log_bin = ON` and `binlog_format = ROW`. If not, configure it:

```bash
# AlmaLinux 9 — MySQL config path
sudo vi /etc/my.cnf
```

Add or update these lines under `[mysqld]`:

```ini
[mysqld]
server-id              = 1
log_bin                = mysql-bin
binlog_format          = ROW
binlog_row_image       = FULL
expire_logs_days       = 7
```

Restart MySQL:

```bash
# AlmaLinux 9
sudo systemctl restart mysqld

# Ubuntu/Debian
sudo systemctl restart mysql
```

**Create `kafka_user` with CDC permissions:**

```bash
mysql -u root -p
```

```sql
CREATE USER IF NOT EXISTS 'kafka_user'@'%' IDENTIFIED BY 'ImR$L007';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'kafka_user'@'%';
CREATE DATABASE IF NOT EXISTS sourcedb;
GRANT ALL PRIVILEGES ON sourcedb.* TO 'kafka_user'@'%';
FLUSH PRIVILEGES;
EXIT;
```

**Create source tables:**

```bash
mysql -u kafka_user -p'ImR$L007' sourcedb < scripts/mysql-init.sql
```

**Contents of `scripts/mysql-init.sql`:**

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

**Alternative: Run via DBeaver (if you prefer a GUI instead of command-line):**

1. Open DBeaver → connect to your VPS MySQL (`62.171.177.208:3306`, user `root`)
2. In the **Database Navigator**, right-click `sourcedb` → **SQL Editor** → **Open SQL Script**
3. Click the folder icon to open a file, navigate to `scripts/mysql-init.sql`, click **Open**
4. Press **Ctrl+A** to select all, then press **Ctrl+Enter** (or click the orange ▶ **Execute SQL Script** button)
5. Verify in DBeaver: expand `sourcedb` → **Tables** → you should see `orders` and `customers` with 2 rows each

> Or paste the SQL directly from the block above into the DBeaver SQL editor and execute.

**Open firewall port 3306:**

```bash
# AlmaLinux 9 (firewalld)
sudo firewall-cmd --permanent --add-port=3306/tcp
sudo firewall-cmd --reload

# Ubuntu (ufw)
sudo ufw allow 3306/tcp
```

**Allow remote connections (check bind-address):**

```bash
# Make sure MySQL listens on all interfaces, not just 127.0.0.1
grep bind-address /etc/my.cnf
# If it shows 127.0.0.1, change to:
# bind-address = 0.0.0.0
sudo systemctl restart mysqld
```

**Verify from Windows:**

```powershell
Test-NetConnection -ComputerName 62.171.177.208 -Port 3306
# TcpTestSucceeded: True = reachable
```

---

## Step 3.2 — Configure VPS PostgreSQL for CDC sink

```bash
# AlmaLinux 9 — PostgreSQL 17 paths
# Data directory: /var/lib/pgsql/17/data/
sudo vi /var/lib/pgsql/17/data/postgresql.conf
```

Set:

```ini
wal_level = logical
max_wal_senders = 10
max_replication_slots = 10
listen_addresses = '*'
```

```bash
# Edit pg_hba.conf to allow remote connections
sudo vi /var/lib/pgsql/17/data/pg_hba.conf
```

Add this line (allows all users from any IP with password auth):

```
host    all    all    0.0.0.0/0    md5
```

```bash
# AlmaLinux 9 — PostgreSQL 17 service name
sudo systemctl restart postgresql-17
```

**Create user, database, schema:**

```bash
sudo -u postgres psql -c "CREATE USER kafka_user WITH PASSWORD 'ImR\$L007';"
sudo -u postgres psql -c "CREATE DATABASE targetdb OWNER kafka_user;"
sudo -u postgres psql -d targetdb -c "CREATE SCHEMA IF NOT EXISTS pipeline AUTHORIZATION kafka_user;"
sudo -u postgres psql -d targetdb -c "GRANT ALL PRIVILEGES ON SCHEMA pipeline TO kafka_user;"
```

> **CRITICAL NOTE — Table ownership:** The tables in the `pipeline` schema MUST be owned by `kafka_user`. If they are created by the `postgres` superuser, the Debezium JDBC Sink connector will fail with `ERROR: must be owner of table orders` when it tries to add CDC columns via `ALTER TABLE`. Either:
> - Let the Debezium sink create the tables automatically (recommended — `schema.evolution: basic` handles this)
> - Or create them manually while connected as `kafka_user`
>
> If you created tables as `postgres` and see the ownership error, drop them and let the sink recreate:
> ```sql
> DROP TABLE IF EXISTS pipeline.orders;
> DROP TABLE IF EXISTS pipeline.customers;
> ```

**Open firewall port 5432:**

```bash
# AlmaLinux 9
sudo firewall-cmd --permanent --add-port=5432/tcp
sudo firewall-cmd --reload

# Ubuntu
sudo ufw allow 5432/tcp
```

**Verify from Windows:**

```powershell
Test-NetConnection -ComputerName 62.171.177.208 -Port 5432
# TcpTestSucceeded: True = reachable
```

---

---

# Phase 4: Docker Infrastructure

---

## Step 4.1 — Create `scripts/mysql-init.sql`

**Why:** When the Docker MySQL container starts for the first time, it automatically runs any `.sql` files in `/docker-entrypoint-initdb.d/`. This script creates the source tables and grants CDC permissions to `kafka_user`. Without the REPLICATION SLAVE grant, Debezium cannot read the binlog.

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

## Step 4.2 — Create `scripts/postgres-init.sql`

**Why:** This script runs automatically when the Docker PostgreSQL container starts. It creates the `pipeline` schema where Debezium will write synced data.

> **Important:** Do NOT create the `pipeline.orders` and `pipeline.customers` tables manually here with extra columns like `_cdc_op` or `_ingested_at`. If you do, the Debezium JDBC Sink connector will try to `ALTER TABLE` to add its own CDC columns and will fail if the existing columns conflict. Let Debezium auto-create the tables via `schema.evolution: basic`.

```sql
-- Grant schema permissions to kafka_user
GRANT ALL PRIVILEGES ON DATABASE targetdb TO kafka_user;

-- Create target schema — Debezium will create tables automatically inside this schema
CREATE SCHEMA IF NOT EXISTS pipeline;
GRANT ALL PRIVILEGES ON SCHEMA pipeline TO kafka_user;
```

Save as: `scripts/postgres-init.sql`

---

## Step 4.3 — Create `monitoring/prometheus.yml`

**Why:** Prometheus needs a config file telling it where to scrape metrics from. Without this file, the Prometheus container will fail to start with a "file not found" mount error.

> **Note:** The `kafka-connect:7072` target will show as DOWN — this is expected. The `debezium/connect:2.6` image does not expose a Prometheus endpoint on port 7072. Use `kafka-exporter:9308` for consumer lag metrics instead (that target will be UP).

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

## Step 4.4 — Create `docker-compose.yml`

**Why this file matters:** This is the master file that defines all 9 containers, their dependencies, ports, environment variables, and health checks. Docker Compose reads it and orchestrates everything.

**Critical notes for Docker 29.x / Compose v5.x:**
- **No `version:` field** — the `version: "3.9"` field is deprecated in Compose v5.x and causes a warning. Remove it entirely.
- **`kafka` service name is used as hostname** — inside the Docker network, all containers reference Kafka as `kafka:9092`. Your Windows machine accesses it as `localhost:9092`.
- **`JsonConverter` not `AvroConverter`** — the `debezium/connect:2.6` image does NOT include Confluent Avro serializer JARs. Using `AvroConverter` causes `ClassNotFoundException`. Use `JsonConverter` throughout.
- **Healthchecks** — `kafka-connect` waits for `kafka` to be healthy before starting, and `kafka-exporter` does the same. Without `condition: service_healthy`, the exporter starts before Kafka is ready and crashes.

---

### Option A — Docker databases (MySQL + PostgreSQL as containers)

Use this if you want everything local, no external server needed.

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

Use this if MySQL and PostgreSQL are on a remote VPS. Remove the `mysql` and `postgres` service blocks and update the volumes section.

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

## Step 4.5 — Pull Docker images (optional but recommended on slow connections)

**Why:** Pulling images one at a time is more stable on slower connections. If a pull fails mid-way, you can retry just that image. `docker compose up` will also pull missing images, but one failure aborts everything.

**Windows PowerShell / Linux / macOS:**

```bash
docker pull confluentinc/cp-kafka:7.6.1
docker pull confluentinc/cp-schema-registry:7.6.1
docker pull debezium/connect:2.6
docker pull provectuslabs/kafka-ui:latest
docker pull danielqsj/kafka-exporter:latest
docker pull prom/prometheus:latest
docker pull grafana/grafana:latest
```

Option A only (Docker databases):

```bash
docker pull mysql:8.0
docker pull postgres:16
```

---

## Step 4.6 — Start all containers

**Why:** `docker compose up -d` starts all services defined in `docker-compose.yml` in detached mode (background). On first run it downloads images and creates volumes (~15 min). Subsequent starts take ~60 seconds.

Run from your project root directory (where `docker-compose.yml` is):

**Windows PowerShell:**

```powershell
docker compose up -d
```

**Linux / macOS:**

```bash
docker compose up -d
```

Watch startup progress:

```bash
docker compose ps
```

Wait until all services show `healthy` or `running`. `kafka-connect` takes the longest (~2–3 minutes).

---

## Step 4.7 — Verify all services are healthy

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

**Windows PowerShell:**

```powershell
# Schema Registry — should return []
curl.exe http://localhost:8081/subjects

# Kafka Connect — should return JSON with version info
curl.exe http://localhost:8083/

# Kafka UI — open in browser
Start-Process "http://localhost:8090"
```

**Linux / macOS:**

```bash
curl http://localhost:8081/subjects    # returns []
curl http://localhost:8083/            # returns {"version":"3.7.0",...}
```

> **Important — curl vs curl.exe on Windows PowerShell:**
> In PowerShell, `curl` is an alias for `Invoke-WebRequest` which shows verbose HTML output and security warnings. Always use `curl.exe` (the real curl binary) for API calls. `curl` works as-is on Linux and macOS.

---

## Phase 4 — Common Errors

**Error: `port 3306 already in use`**
- Your local MySQL is running on port 3306.
- Fix: Change the MySQL container mapping to `"3307:3306"` in `docker-compose.yml` (already done in the config above).

**Error: `port 8080 already in use`**
- Another service (Spring Boot, Tomcat) is using port 8080.
- Fix: Use `"8090:8080"` for Kafka UI (already done in the config above).

**Error: `monitoring/prometheus.yml: no such file`**
- The prometheus volume mount fails if the file doesn't exist on the host.
- Fix: Create `monitoring/prometheus.yml` first (Step 4.3), then run `docker compose up -d`.

**Error: `TLS handshake timeout` during image pull**
- Network issue. Fix: Retry `docker compose up -d` — Docker resumes from cached layers.
- Or pull images one at a time (Step 4.5).

**Error: `CLUSTER_ID mismatch` on restart**
- Kafka's data volume has a different cluster ID from `docker-compose.yml`.
- Fix: `docker compose down -v` (removes volumes), then `docker compose up -d`.

**kafka-exporter keeps restarting**
- It started before Kafka was fully ready.
- Fix: `docker compose restart kafka-exporter` after Kafka shows `healthy`.

---

---

# Phase 5: Kafka Topics

**Why create topics manually?**
Kafka is configured with `KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"`. This prevents connectors from accidentally creating topics with wrong partition counts or retention settings. We create them explicitly with the correct configuration before registering connectors.

---

## Step 5.1 — Create all required topics

**Windows PowerShell:**

```powershell
# CDC source topics — one per MySQL table being captured
docker exec kafka kafka-topics --create --bootstrap-server localhost:9092 --topic prod.mysql.sourcedb.orders --partitions 3 --replication-factor 1 --config retention.ms=604800000 --if-not-exists

docker exec kafka kafka-topics --create --bootstrap-server localhost:9092 --topic prod.mysql.sourcedb.customers --partitions 3 --replication-factor 1 --config retention.ms=604800000 --if-not-exists

# Dead Letter Queue — retains failed messages forever (-1) for investigation
docker exec kafka kafka-topics --create --bootstrap-server localhost:9092 --topic prod.dlq.errors --partitions 1 --replication-factor 1 --config retention.ms=-1 --if-not-exists

# Kafka Connect internal topics — required for connector config/offset/status storage
docker exec kafka kafka-topics --create --bootstrap-server localhost:9092 --topic _connect-configs --partitions 1 --replication-factor 1 --if-not-exists

docker exec kafka kafka-topics --create --bootstrap-server localhost:9092 --topic _connect-offsets --partitions 25 --replication-factor 1 --if-not-exists

docker exec kafka kafka-topics --create --bootstrap-server localhost:9092 --topic _connect-status --partitions 5 --replication-factor 1 --if-not-exists

# Required by Debezium heartbeat — without this you get UNKNOWN_TOPIC_OR_PARTITION errors
docker exec kafka kafka-topics --create --bootstrap-server localhost:9092 --topic __debezium-heartbeat.prod.mysql --partitions 1 --replication-factor 1 --if-not-exists

# Required by Debezium schema changes (include.schema.changes: true)
docker exec kafka kafka-topics --create --bootstrap-server localhost:9092 --topic prod.mysql --partitions 1 --replication-factor 1 --if-not-exists
```

**Linux / macOS (Git Bash on Windows also works):**

```bash
# Run all at once with a loop
for TOPIC in \
  "prod.mysql.sourcedb.orders:3" \
  "prod.mysql.sourcedb.customers:3" \
  "prod.mysql.sourcedb.products:3" \
  "prod.dlq.errors:1" \
  "_connect-configs:1" \
  "_connect-offsets:25" \
  "_connect-status:5" \
  "__debezium-heartbeat.prod.mysql:1" \
  "prod.mysql:1"; do
  NAME=$(echo $TOPIC | cut -d: -f1)
  PARTS=$(echo $TOPIC | cut -d: -f2)
  docker exec kafka kafka-topics --create \
    --bootstrap-server localhost:9092 \
    --topic "$NAME" \
    --partitions $PARTS \
    --replication-factor 1 \
    --if-not-exists
  echo "Created: $NAME"
done
```

---

## Step 5.2 — Verify topics exist

```bash
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092
```

Or open Kafka UI: http://localhost:8090 → **Topics**

---

## Phase 5 — Common Errors

**Error: `UNKNOWN_TOPIC_OR_PARTITION: __debezium-heartbeat.prod.mysql`**
- This error appears in `docker logs kafka-connect` when the heartbeat topic is missing.
- Fix: Create the `__debezium-heartbeat.prod.mysql` topic (included in Step 5.1 above).

**Error: `UNKNOWN_TOPIC_OR_PARTITION: prod.mysql`**
- Missing topic for schema change events (`include.schema.changes: true`).
- Fix: Create the `prod.mysql` topic (included in Step 5.1 above).

---

---

# Phase 6: Source Connector — MySQL CDC

---

## Step 6.1 — Understanding the source connector config

Before creating the file, understand the key settings:

| Setting | Value | Why |
|---------|-------|-----|
| `connector.class` | `io.debezium.connector.mysql.MySqlConnector` | The MySQL CDC connector class bundled in `debezium/connect:2.6` |
| `key.converter.schemas.enable` | `true` | **CRITICAL** — The Debezium JDBC Sink connector needs schema info embedded in messages. Setting this to `false` causes `valueSchema() is null` errors and the sink writes 0 rows silently. |
| `value.converter.schemas.enable` | `true` | Same as above — both key and value schemas must be included. |
| `transforms.unwrap.drop.tombstones` | `true` | **CRITICAL** — When MySQL deletes a row, Debezium sends 2 messages: a rewrite record (`__deleted: true`) and a tombstone (null value). The Debezium JDBC Sink crashes on tombstone messages with `primary key mode 'record_value' cannot have null schema`. Setting this to `true` drops tombstones before they reach the sink. |
| `transforms.unwrap.delete.handling.mode` | `rewrite` | Converts DELETE events to UPDATE events with `__deleted: true`. The row stays in PostgreSQL but is marked as deleted (soft delete / audit trail). |
| `snapshot.mode` | `initial` | Takes a full snapshot of existing data on first start, then streams new changes. If connector offsets already exist (from a previous run), the snapshot is SKIPPED. Only run on first registration. |
| `database.server.id` | `184054` | The server ID Debezium uses to register as a MySQL replica. Must be unique across all MySQL replicas. Any number not used by other replicas. |

---

## Step 6.2 — Create `connectors/source/mysql-cdc-source.json`

### Option A — Docker MySQL

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
    "key.converter.schemas.enable": "true",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "true",

    "transforms": "unwrap,addMetadata",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.add.fields": "op,ts_ms,source.db,source.table",
    "transforms.unwrap.delete.handling.mode": "rewrite",
    "transforms.unwrap.drop.tombstones": "true",

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

### Option B — VPS MySQL

Change only these fields:

```json
{
  "name": "mysql-cdc-source",
  "config": {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "tasks.max": "1",

    "database.hostname": "62.171.177.208",
    "database.port": "3306",
    "database.user": "kafka_user",
    "database.password": "YOUR_PASSWORD",
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
    "key.converter.schemas.enable": "true",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "true",

    "transforms": "unwrap,addMetadata",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.add.fields": "op,ts_ms,source.db,source.table",
    "transforms.unwrap.delete.handling.mode": "rewrite",
    "transforms.unwrap.drop.tombstones": "true",

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

| Field | Option A (Docker) | Option B (VPS) |
|-------|------------------|----------------|
| `database.hostname` | `"mysql"` | `"62.171.177.208"` e.g. `"62.171.177.208"` |
| `database.password` | `"kafka_password"` | `"YOUR_PASSWORD"` |

---

## Step 6.3 — Register the source connector

**Why:** The connector config is registered via the Kafka Connect REST API. Kafka Connect stores it in the `_connect-configs` Kafka topic, so it survives restarts automatically. You only need to register once — after that it auto-starts on `docker compose up`.

**Windows PowerShell:**

```powershell
curl.exe -X POST http://localhost:8083/connectors `
  -H "Content-Type: application/json" `
  -d "@connectors/source/mysql-cdc-source.json"
```

Alternative (native PowerShell, no curl.exe needed):

```powershell
$body = Get-Content connectors/source/mysql-cdc-source.json -Raw
Invoke-RestMethod -Method Post `
  -Uri "http://localhost:8083/connectors" `
  -ContentType "application/json" `
  -Body $body
```

**Linux / macOS / Git Bash:**

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @connectors/source/mysql-cdc-source.json
```

Expected response: the connector config echoed back as JSON with no `error_code` field.

---

## Step 6.4 — Verify source connector is running

```bash
# Windows PowerShell
curl.exe http://localhost:8083/connectors/mysql-cdc-source/status

# Linux / macOS
curl http://localhost:8083/connectors/mysql-cdc-source/status
```

Expected:

```json
{
  "name": "mysql-cdc-source",
  "connector": { "state": "RUNNING" },
  "tasks": [{ "id": 0, "state": "RUNNING" }],
  "type": "source"
}
```

Both `connector.state` and `tasks[0].state` must be `RUNNING`.

---

## Step 6.5 — Verify snapshot data is in Kafka

After the connector starts, Debezium takes a snapshot of all existing rows in MySQL and publishes them to Kafka. This happens automatically within ~30 seconds.

```bash
# Check message count in orders topic
docker exec kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic prod.mysql.sourcedb.orders
```

You should see non-zero offsets (one offset per partition).

Check actual message content:

```bash
# Windows PowerShell
docker exec kafka kafka-console-consumer `
  --bootstrap-server localhost:9092 `
  --topic prod.mysql.sourcedb.orders `
  --from-beginning `
  --max-messages 3 `
  --timeout-ms 10000

# Linux / macOS
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic prod.mysql.sourcedb.orders \
  --from-beginning \
  --max-messages 3 \
  --timeout-ms 10000
```

Each message should be a large JSON object with both `"schema"` and `"payload"` fields. If you see raw binary data, the converter is wrong (check `schemas.enable` settings).

---

## Phase 6 — Common Errors

**Error: connector state is FAILED**

Check the trace:

```bash
curl.exe http://localhost:8083/connectors/mysql-cdc-source/status
```

Look at the `trace` field in the response. Common causes:
- MySQL not reachable — check hostname and port
- Wrong credentials — check `database.user` and `database.password`
- Binlog not enabled — check with `SHOW VARIABLES LIKE 'log_bin';`

**Error: `Connector mysql-cdc-source already exists` (error_code: 409)**

- Connector is already registered and running. This is fine.
- Kafka Connect stores connector configs in the `_connect-configs` Kafka topic, so they survive `docker compose down/up`. You only need to register once.
- If you want to update the config: delete and re-register.

**Error: Messages in Kafka have no schema (just raw values)**

- Cause: `schemas.enable: false` somewhere in the config.
- Fix: Ensure both `key.converter.schemas.enable: true` AND `value.converter.schemas.enable: true` are set in the connector JSON.
- After fixing, delete the connector + topics + recreate (old schemaless messages must be cleared).

**Snapshot.mode skips existing data on re-registration**

- `snapshot.mode: initial` only snapshots if NO stored offsets exist for this connector name.
- If you delete and re-register with the SAME connector name, Debezium finds the existing offsets in `_connect-offsets` and skips the snapshot — only new changes (INSERT/UPDATE/DELETE after registration) will be captured.
- Fix option 1: Trigger real MySQL UPDATEs to force CDC events for existing rows.
- Fix option 2: Use a new connector name to force a fresh snapshot (see Troubleshooting section).

---

---

# Phase 7: Sink Connector — PostgreSQL

---

## Step 7.1 — Understanding the sink connector config

**Why Debezium JDBC Sink, not Confluent JDBC Sink?**

The `debezium/connect:2.6` image bundles `io.debezium.connector.jdbc.JdbcSinkConnector`. The Confluent `io.confluent.connect.jdbc.JdbcSinkConnector` is NOT included. Attempting to use the Confluent class name causes `connector.class not found` errors.

| Property | Confluent JDBC (WRONG for this image) | Debezium JDBC (CORRECT) |
|----------|--------------------------------------|------------------------|
| connector class | `io.confluent.connect.jdbc.JdbcSinkConnector` | `io.debezium.connector.jdbc.JdbcSinkConnector` |
| username | `connection.user` | `connection.username` |
| primary key | `pk.mode` | `primary.key.mode` |
| pk fields | `pk.fields` | `primary.key.fields` |
| auto create | `auto.create: true` | `schema.evolution: basic` |

**What the RegexRouter transform does:**

The Kafka topic name is `prod.mysql.sourcedb.orders`. Without transformation, the sink would try to create a table named `prod.mysql.sourcedb.orders` (which is invalid in PostgreSQL). The RegexRouter maps `prod.mysql.sourcedb.orders` → `orders`, and then `table.name.format: pipeline.${topic}` makes the full table name `pipeline.orders`.

---

## Step 7.2 — Create `connectors/sink/postgres-sink.json`

### Option A — Docker PostgreSQL

```json
{
  "name": "postgres-sink",
  "config": {
    "connector.class": "io.debezium.connector.jdbc.JdbcSinkConnector",
    "tasks.max": "2",

    "connection.url": "jdbc:postgresql://postgres:5432/targetdb",
    "connection.username": "kafka_user",
    "connection.password": "kafka_password",

    "topics": "prod.mysql.sourcedb.orders,prod.mysql.sourcedb.customers",

    "insert.mode": "upsert",
    "primary.key.mode": "record_value",
    "primary.key.fields": "id",

    "schema.evolution": "basic",
    "table.name.format": "pipeline.${topic}",

    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "true",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "true",

    "transforms": "router",
    "transforms.router.type": "org.apache.kafka.connect.transforms.RegexRouter",
    "transforms.router.regex": "prod\\.mysql\\.sourcedb\\.(.*)",
    "transforms.router.replacement": "$1",

    "batch.size": "3000",
    "max.retries": "10",
    "retry.backoff.ms": "3000",

    "errors.tolerance": "all",
    "errors.log.enable": "true",
    "errors.deadletterqueue.topic.name": "prod.dlq.errors"
  }
}
```

### Option B — VPS PostgreSQL

Change only these fields:

| Field | Option A (Docker) | Option B (VPS) |
|-------|------------------|----------------|
| `connection.url` | `"jdbc:postgresql://postgres:5432/targetdb"` | `"jdbc:postgresql://62.171.177.208:5432/targetdb"` |
| `connection.password` | `"kafka_password"` | `"YOUR_PASSWORD"` |

Example VPS values:

```json
"connection.url": "jdbc:postgresql://62.171.177.208:5432/targetdb",
"connection.username": "kafka_user",
"connection.password": "YOUR_PASSWORD",
```

> **Test connectivity before registering:**
> ```powershell
> # Windows
> Test-NetConnection -ComputerName 62.171.177.208 -Port 5432
> # TcpTestSucceeded: True = reachable
> ```

---

## Step 7.3 — Register the sink connector

**Windows PowerShell:**

```powershell
curl.exe -X POST http://localhost:8083/connectors `
  -H "Content-Type: application/json" `
  -d "@connectors/sink/postgres-sink.json"
```

**Linux / macOS / Git Bash:**

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @connectors/sink/postgres-sink.json
```

---

## Step 7.4 — Verify sink connector is running

```bash
# Windows PowerShell
curl.exe http://localhost:8083/connectors/postgres-sink/status

# Linux / macOS
curl http://localhost:8083/connectors/postgres-sink/status
```

Expected: both `connector.state` and all `tasks[*].state` are `RUNNING`.

---

## Step 7.5 — Verify data synced to PostgreSQL

**Option A (Docker PostgreSQL):**

```bash
# Windows PowerShell
docker exec postgres-target psql -U kafka_user -d targetdb -c "SELECT * FROM pipeline.orders ORDER BY id;"

# Linux / macOS
docker exec postgres-target psql -U kafka_user -d targetdb -c "SELECT * FROM pipeline.orders ORDER BY id;"
```

**Option B (VPS PostgreSQL — run on VPS):**

```bash
psql -U kafka_user -d targetdb -c "SELECT * FROM pipeline.orders ORDER BY id;"
```

You should see all rows from MySQL mirrored in PostgreSQL with extra columns: `__deleted`, `__op`, `__ts_ms`, `__source_db`, `__source_table`, `_pipeline_version`.

---

## Step 7.6 — Understanding DELETE behaviour

**What happens:** MySQL DELETEs are captured as soft deletes in PostgreSQL. The row is NOT removed — instead it gets `__deleted = "true"` and `__op = "d"`.

**Why:** This is a deliberate design choice for audit trails. You always know what data existed and when it was deleted. If you want hard deletes (physical row removal), you need a custom consumer (Phase 9) instead of the JDBC sink connector.

Test it:

```bash
# Option A — Docker MySQL
docker exec mysql-source mysql -u kafka_user -pkafka_password sourcedb -e "DELETE FROM orders WHERE id = 1;"

# Option B — VPS MySQL (run on VPS)
mysql -u kafka_user -p'YOUR_PASSWORD' sourcedb -e "DELETE FROM orders WHERE id = 1;"
```

After ~3 seconds, check PostgreSQL:

```bash
# Should show __deleted = true, __op = d for id=1
docker exec postgres-target psql -U kafka_user -d targetdb \
  -c "SELECT id, __op, __deleted FROM pipeline.orders WHERE id = 1;"
```

---

## Phase 7 — Common Errors

**Error: `io.confluent.connect.jdbc.JdbcSinkConnector not found`**
- You used the Confluent connector class name.
- Fix: Use `io.debezium.connector.jdbc.JdbcSinkConnector`.

**Error: `valueSchema() is null` — sink writes 0 rows**
- Cause: Messages in the Kafka topic were produced with `schemas.enable: false`.
- Fix: Set `key.converter.schemas.enable: true` and `value.converter.schemas.enable: true` on BOTH the source and sink connectors. Then do a full clean reset (see Troubleshooting).

**Error: `primary key mode 'record_value' cannot have null schema` — sink FAILED**
- Cause: Tombstone messages (null-value records) from MySQL DELETEs are in the topic.
- Fix: Ensure source connector has `"transforms.unwrap.drop.tombstones": "true"`. For recovery from existing tombstones, see the Troubleshooting section.

**Error: `must be owner of table orders` (VPS PostgreSQL)**
- Cause: The `pipeline.orders` table was created by the `postgres` superuser, not by `kafka_user`. The sink tries to `ALTER TABLE` to add CDC columns but `kafka_user` doesn't have permission.
- Fix: Drop the tables and let the sink recreate them as `kafka_user`:
  ```sql
  -- Run on VPS PostgreSQL
  DROP TABLE IF EXISTS pipeline.orders;
  DROP TABLE IF EXISTS pipeline.customers;
  ```
  Then restart the sink connector.

**Sink RUNNING but 0 rows in PostgreSQL after switching from Docker to VPS MySQL**
- Cause: The source connector has stale binlog offsets from Docker MySQL stored in `_connect-offsets`. When it connects to VPS MySQL, it tries to resume from the wrong binlog position and misses all events.
- Fix: Rename the connector to force a fresh snapshot (see Troubleshooting section).

**Error: `duplicate key value violates unique constraint "pg_type_typname_nsp_index"` — table never created**
- Cause: PostgreSQL registers a type in `pg_type` when a table is created. If a previous `CREATE TABLE` attempt failed mid-way (e.g., due to a permission error), it leaves behind a dangling type with the table name. On the next attempt the connector tries to create the type again and hits a duplicate key conflict — the table is never created and the connector loops endlessly.
- Why `schema.evolution: basic` worked before but not now: On a clean first run it works fine. It breaks when a previous failed run already left a dangling type. Each retry makes it worse — the type accumulates but the table is never created.
- Fix (two steps):
  1. Drop the dangling types and grant schema permissions on VPS PostgreSQL (as superuser):
     ```sql
     -- Run as postgres superuser on VPS
     DROP TYPE IF EXISTS pipeline.orders CASCADE;
     DROP TYPE IF EXISTS pipeline.customers CASCADE;
     GRANT ALL ON SCHEMA pipeline TO kafka_user;
     ```
  2. Pre-create the tables manually as `kafka_user` and set `schema.evolution: none` in the connector config to prevent the connector from attempting table creation again:
     ```sql
     CREATE TABLE IF NOT EXISTS pipeline.orders (
       id bigint NOT NULL,
       customer_id bigint NOT NULL,
       product_id bigint NOT NULL,
       quantity integer NOT NULL,
       amount decimal(10,2) NOT NULL,
       status text DEFAULT 'PENDING',
       created_at timestamptz,
       updated_at timestamptz,
       __deleted text, __op text, __ts_ms bigint,
       __source_db text, __source_table text, _pipeline_version text,
       PRIMARY KEY(id)
     );
     CREATE TABLE IF NOT EXISTS pipeline.customers (
       id bigint NOT NULL PRIMARY KEY,
       name text, email text,
       __deleted text, __op text, __ts_ms bigint,
       __source_db text, __source_table text, _pipeline_version text
     );
     GRANT ALL ON pipeline.orders TO kafka_user;
     GRANT ALL ON pipeline.customers TO kafka_user;
     ```
  3. In `connectors/sink/postgres-sink.json` change `"schema.evolution": "basic"` → `"schema.evolution": "none"`.
  4. Delete the connector, reset consumer group offsets to earliest, and re-register:
     ```powershell
     Invoke-WebRequest -Method DELETE -Uri http://localhost:8083/connectors/postgres-sink
     docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --group connect-postgres-sink --reset-offsets --to-earliest --all-topics --execute
     curl.exe -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d "@connectors/sink/postgres-sink.json"
     ```

---

---

# Phase 8: Syncing Existing Production Tables (Real-World Setup)

**This is the most common real-world scenario:** Both your MySQL source table and PostgreSQL target table already exist with production data. You want to set up the CDC pipeline between them without losing existing data.

---

## The Challenge

In a fresh setup, Debezium creates the PostgreSQL table automatically. In the real world:
- MySQL `sourcedb.orders` already has 10,000+ rows
- PostgreSQL `targetdb.pipeline.orders` already exists (created by DBA, another app, or manually)
- Column names and types may differ slightly
- The existing PostgreSQL table may be owned by a different user (not `kafka_user`)

---

## Step 8.1 — Check existing table schemas match

**On VPS MySQL (SSH into 62.171.177.208):**

```bash
mysql -u kafka_user -p'YOUR_PASSWORD' sourcedb -e "DESCRIBE orders;"
```

**On VPS PostgreSQL:**

```bash
psql -U kafka_user -d targetdb -c "\d pipeline.orders"
```

Compare columns. The PostgreSQL table must have at minimum the same columns as MySQL. Debezium will also add these CDC columns automatically via `ALTER TABLE`:
- `__deleted` (text)
- `__op` (text)
- `__ts_ms` (bigint)
- `__source_db` (text)
- `__source_table` (text)
- `_pipeline_version` (text)

---

## Step 8.2 — Fix table ownership (most common issue)

**Why this matters:** Debezium sink needs to `ALTER TABLE` to add the CDC columns above. If the table is owned by `postgres` (superuser) instead of `kafka_user`, you get:
```
ERROR: must be owner of table orders
```

**Check current owner:**

```bash
psql -U postgres -d targetdb -c "\dt pipeline.*"
# Look at the "Owner" column
```

**Option A — Transfer ownership to kafka_user (keeps existing data):**

```bash
sudo -u postgres psql -d targetdb -c "ALTER TABLE pipeline.orders OWNER TO kafka_user;"
sudo -u postgres psql -d targetdb -c "ALTER TABLE pipeline.customers OWNER TO kafka_user;"
```

**Option B — Drop and let Debezium recreate (loses existing PG data, resync from MySQL):**

```sql
-- Run as postgres superuser
DROP TABLE IF EXISTS pipeline.orders;
DROP TABLE IF EXISTS pipeline.customers;
```

> Use Option A if you have existing PostgreSQL data you want to keep.
> Use Option B if PostgreSQL is just a replica and MySQL is the source of truth.

---

## Step 8.3 — Ensure kafka_user has schema privileges

```bash
sudo -u postgres psql -d targetdb -c "GRANT ALL PRIVILEGES ON SCHEMA pipeline TO kafka_user;"
sudo -u postgres psql -d targetdb -c "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA pipeline TO kafka_user;"
```

---

## Step 8.4 — Point the source connector to your existing MySQL table

Edit `connectors/source/mysql-cdc-source.json` — set `table.include.list` to your actual table(s):

```json
"database.include.list": "sourcedb",
"table.include.list": "sourcedb.orders,sourcedb.customers"
```

Example for this project:

```json
"database.hostname": "62.171.177.208",
"database.port": "3306",
"database.user": "kafka_user",
"database.password": "YOUR_PASSWORD",
"database.include.list": "sourcedb",
"table.include.list": "sourcedb.orders,sourcedb.customers"
```

---

## Step 8.5 — Point the sink connector to your existing PostgreSQL table

Edit `connectors/sink/postgres-sink.json`:

```json
"connection.url": "jdbc:postgresql://62.171.177.208:5432/targetdb",
"connection.username": "kafka_user",
"connection.password": "YOUR_PASSWORD",
"topics": "prod.mysql.sourcedb.orders,prod.mysql.sourcedb.customers"
```

The `RegexRouter` maps topic `prod.mysql.sourcedb.orders` → table name `orders`, and `table.name.format: pipeline.${topic}` makes the final table `pipeline.orders`. This means your existing `pipeline.orders` table will receive the synced data.

---

## Step 8.6 — Register connectors and let snapshot run

```powershell
# Register source — Debezium will snapshot ALL existing MySQL rows into Kafka
curl.exe -X POST http://localhost:8083/connectors `
  -H "Content-Type: application/json" `
  -d "@connectors/source/mysql-cdc-source.json"
```

Watch logs to confirm snapshot is running:

```powershell
docker logs kafka-connect --tail 30
# Look for: "Snapshot step 7 - Snapshotting data"
# And: "Finished exporting N records for table 'sourcedb.orders'"
```

Wait for snapshot to complete (~30 seconds for small tables, longer for large tables), then register sink:

```powershell
curl.exe -X POST http://localhost:8083/connectors `
  -H "Content-Type: application/json" `
  -d "@connectors/sink/postgres-sink.json"
```

---

## Step 8.7 — Verify existing data synced

```bash
# Count rows in MySQL
mysql -u kafka_user -p'YOUR_PASSWORD' sourcedb -e "SELECT COUNT(*) FROM orders;"

# Count rows in PostgreSQL (should match after sync)
psql -U kafka_user -d targetdb -c "SELECT COUNT(*) FROM pipeline.orders;"
```

After the sink processes the snapshot messages from Kafka, the row counts should match.

---

## Step 8.8 — Handle column type mismatches

If the sink fails with a column type error (e.g., MySQL has `DECIMAL(10,2)` but PostgreSQL has `INTEGER`), you have two options:

**Option A — Alter the PostgreSQL column to match MySQL:**

```sql
-- Run as kafka_user on PostgreSQL
ALTER TABLE pipeline.orders ALTER COLUMN amount TYPE NUMERIC(10,2);
```

**Option B — Drop and let Debezium recreate with correct types:**

```sql
DROP TABLE IF EXISTS pipeline.orders;
-- Restart the sink connector — it will recreate the table with correct types
```

> **Note about the `amount` / `DECIMAL` field:** Debezium represents MySQL `DECIMAL` columns as `bytes` (Base64-encoded) in JSON format. In PostgreSQL the value may appear as `"FXw="` instead of `49.99`. This is a known Debezium behavior with JsonConverter. The actual numeric value is stored correctly when using the Debezium JDBC Sink connector — it decodes the bytes. If you query via SQL you will see the correct number.

---

## Step 8.9 — Schema mismatch: existing PostgreSQL table has extra columns

If your existing PostgreSQL table has columns that MySQL doesn't have (e.g., `created_by`, `tenant_id`), the Debezium sink will skip those columns (they keep their existing values or defaults). This is fine — Debezium only manages the columns it knows about from MySQL.

If your existing PostgreSQL table is **missing** columns that MySQL has, Debezium will `ALTER TABLE ADD COLUMN` automatically (this is what `schema.evolution: basic` does).

---

## Summary: Checklist for existing table sync

- [ ] `kafka_user` has `REPLICATION SLAVE` grant on VPS MySQL
- [ ] VPS MySQL `log_bin = ON` and `binlog_format = ROW`
- [ ] `pipeline.orders` table owned by `kafka_user` on VPS PostgreSQL
- [ ] `kafka_user` has `ALL PRIVILEGES ON SCHEMA pipeline`
- [ ] Source connector registered — snapshot running
- [ ] Snapshot completed (check `docker logs kafka-connect`)
- [ ] Sink connector registered — status RUNNING
- [ ] Row counts match between MySQL and PostgreSQL
- [ ] Test INSERT in MySQL → appears in PostgreSQL within 5 seconds

---

---

# Phase 9: Full Clean Reset (When Things Go Wrong)

**Use this procedure when:**
- The Kafka topic has mixed data (from multiple MySQL sources or corrupt messages)
- The sink is processing wrong data or 0 rows despite connector RUNNING
- You want to start completely fresh with a new MySQL source

---

## Full clean reset procedure

**Windows PowerShell:**

```powershell
# Step 1: Delete both connectors
curl.exe -X DELETE http://localhost:8083/connectors/mysql-cdc-source
curl.exe -X DELETE http://localhost:8083/connectors/postgres-sink

# Step 2: Delete CDC topics (removes all messages including corrupt/mixed data)
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic prod.mysql.sourcedb.orders
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic prod.mysql.sourcedb.customers

# Step 3: Recreate topics clean
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic prod.mysql.sourcedb.orders --partitions 3 --replication-factor 1
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic prod.mysql.sourcedb.customers --partitions 3 --replication-factor 1

# Step 4: Re-register source (will do a fresh snapshot)
curl.exe -X POST http://localhost:8083/connectors `
  -H "Content-Type: application/json" `
  -d "@connectors/source/mysql-cdc-source.json"

# Step 5: Wait 20 seconds for snapshot to complete, then register sink
Start-Sleep -Seconds 20
curl.exe -X POST http://localhost:8083/connectors `
  -H "Content-Type: application/json" `
  -d "@connectors/sink/postgres-sink.json"
```

**Linux / macOS:**

```bash
# Steps 1-5 equivalent
curl -X DELETE http://localhost:8083/connectors/mysql-cdc-source
curl -X DELETE http://localhost:8083/connectors/postgres-sink

docker exec kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic prod.mysql.sourcedb.orders
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic prod.mysql.sourcedb.customers

docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic prod.mysql.sourcedb.orders --partitions 3 --replication-factor 1
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic prod.mysql.sourcedb.customers --partitions 3 --replication-factor 1

curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @connectors/source/mysql-cdc-source.json

sleep 20

curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @connectors/sink/postgres-sink.json
```

Also drop and recreate PostgreSQL target tables so the sink recreates them with proper ownership:

```sql
-- Run on PostgreSQL (Docker or VPS)
DROP TABLE IF EXISTS pipeline.orders;
DROP TABLE IF EXISTS pipeline.customers;
```

---

## Forcing a fresh snapshot when switching MySQL sources

**Problem:** If you change `database.hostname` in the source connector from Docker MySQL to VPS MySQL (or vice versa) and re-register with the same connector name, the old binlog offsets from the previous MySQL server are still stored in `_connect-offsets`. Debezium reads those offsets and tries to resume from a position that doesn't exist on the new MySQL server. It will appear to run without errors but new CDC events won't appear in Kafka.

**Solution:** Use a new connector name to force Debezium to treat it as a brand new registration with no existing offsets:

1. Edit `connectors/source/mysql-cdc-source.json` — change `"name"` to a new unique name, e.g. `"mysql-cdc-source-v2"`
2. Delete the old connector: `curl.exe -X DELETE http://localhost:8083/connectors/mysql-cdc-source`
3. Register the new connector: `curl.exe -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d "@connectors/source/mysql-cdc-source.json"`
4. Debezium will do a fresh snapshot of the new MySQL server

---

---

# Phase 9: Troubleshooting Reference

---

## Tombstone messages causing sink FAILED

**What happened:** MySQL DELETE generates two Kafka messages:
1. A rewrite record: `{ ..., "__deleted": "true", "__op": "d" }` — this is the row data with deleted flag
2. A tombstone: `null` (completely null value) — used for Kafka log compaction

The Debezium JDBC Sink crashes on the tombstone message with:
```
primary key mode 'record_value' cannot have null schema
```

**Prevention:** Set `"transforms.unwrap.drop.tombstones": "true"` in the source connector (already in the config above).

**Recovery if tombstones are already in the topic:**

```powershell
# 1. Delete the sink connector
curl.exe -X DELETE http://localhost:8083/connectors/postgres-sink

# 2. Reset consumer group offsets to LATEST (skips all existing tombstone messages)
docker exec kafka kafka-consumer-groups `
  --bootstrap-server localhost:9092 `
  --group connect-postgres-sink `
  --reset-offsets --to-latest `
  --topic prod.mysql.sourcedb.orders --execute

docker exec kafka kafka-consumer-groups `
  --bootstrap-server localhost:9092 `
  --group connect-postgres-sink `
  --reset-offsets --to-latest `
  --topic prod.mysql.sourcedb.customers --execute

# 3. Re-register the sink (will start from latest, processing only new messages)
curl.exe -X POST http://localhost:8083/connectors `
  -H "Content-Type: application/json" `
  -d "@connectors/sink/postgres-sink.json"
```

> **Note after tombstone recovery:** The sink now starts from the latest offset, so historical data already in the topic is skipped. Do a fresh INSERT or UPDATE in MySQL to verify the pipeline is working. If you need all historical data, do a full clean reset (Phase 8) instead.

---

## `schemas.enable: false` — sink writes 0 rows silently

**What happened:** The Debezium JDBC Sink connector needs schema information embedded in each Kafka message to know column types and names. If messages were produced with `schemas.enable: false` (stripped schema), the sink receives messages with `valueSchema() = null` and silently processes 0 rows.

**How to detect:**

```bash
# Check a raw message from the topic
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic prod.mysql.sourcedb.orders \
  --from-beginning \
  --max-messages 1 \
  --timeout-ms 5000
```

If the message starts with `{"schema":{"type":"struct",...},"payload":{...}}` → schemas are embedded ✓

If the message starts with `{"id":1,"customer_id":...}` (just payload, no schema wrapper) → schemas are missing ✗

**Fix:** Set `key.converter.schemas.enable: true` AND `value.converter.schemas.enable: true` in both source and sink connector JSON. Then do a full clean reset (Phase 8) to clear the old schemaless messages from the topic.

---

## Bad message in Kafka topic causing deserialization errors

**What happened:** A plain-text or non-JSON message (e.g., `hello`) was published to a CDC topic. The JsonConverter can't parse it and logs:
```
Unrecognized token 'hello': was expecting JSON String, Number, Array, Object...
```

**How to detect:**

```bash
docker logs kafka-connect --tail 50
# Look for: JsonParseException or SerializationException
```

**Fix:**

The sink connector's `errors.tolerance: all` should route bad messages to the DLQ (`prod.dlq.errors`). If the sink is stuck:

```bash
# Check DLQ for the bad messages
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic prod.dlq.errors \
  --from-beginning \
  --max-messages 10 \
  --timeout-ms 5000
```

If the sink is FAILED, use the tombstone recovery procedure above to reset offsets to latest and skip the bad message.

---

## Connector already exists (409 error)

**What happened:** You tried to register a connector that's already registered.

```json
{"error_code":409,"message":"Connector mysql-cdc-source already exists"}
```

**What to do:** Nothing — this is expected. Connector configs are stored in Kafka's `_connect-configs` topic and survive `docker compose down/up`. The connector auto-restarts when Kafka Connect comes back up. Check status to confirm it's running:

```bash
curl.exe http://localhost:8083/connectors/mysql-cdc-source/status
```

---

## Checking connector logs for errors

```bash
# Last 50 lines of Kafka Connect logs
docker logs kafka-connect --tail 50

# Windows PowerShell — filter for errors (no grep, use Select-String)
docker logs kafka-connect --tail 100 2>&1 | Select-String -Pattern "ERROR|WARN|Exception"

# Linux / macOS
docker logs kafka-connect --tail 100 2>&1 | grep -i "error\|exception\|failed"
```

---

## Complete infrastructure teardown and restart

```bash
# Stop and remove all containers + volumes (complete clean slate)
docker compose down -v

# Restart fresh
docker compose up -d
```

> **Warning:** `docker compose down -v` removes ALL data volumes including Kafka data, MySQL data, and PostgreSQL data. After this you need to recreate topics AND re-register connectors.

---

---

# Phase 10: Python Producer/Consumer Scripts

---

## Step 10.1 — Create CSV to Kafka producer: `scripts/csv_producer.py`

**What it does:** Reads a CSV file and publishes each row to a Kafka topic as a JSON message. Useful for loading bulk data files into the pipeline.

```python
# scripts/csv_producer.py
import csv
import json
import uuid
import sys
from pathlib import Path
from confluent_kafka import Producer

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "prod.files.csv.raw"

def delivery_report(err, msg):
    if err:
        print(f"Delivery failed for row {msg.key()}: {err}")

def produce_csv(file_path: str):
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    file_name = Path(file_path).name
    row_num = 0

    with open(file_path, newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row_num, row in enumerate(reader, start=1):
            row["_source_file"] = file_name
            row["_row_number"] = row_num

            producer.produce(
                topic=TOPIC,
                key=str(uuid.uuid4()),
                value=json.dumps(row),
                on_delivery=delivery_report
            )

            if row_num % 1000 == 0:
                producer.poll(0)
                print(f"Produced {row_num} rows...")

    producer.flush()
    print(f"Done. Produced {row_num} rows from {file_name} to topic {TOPIC}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python scripts/csv_producer.py <path/to/file.csv>")
        sys.exit(1)
    produce_csv(sys.argv[1])
```

Create a sample CSV to test it — `scripts/sample_data.csv`:

```csv
id,name,amount,created_at
1,Widget A,19.99,2024-01-15 10:00:00
2,Widget B,34.50,2024-01-15 10:05:00
3,Widget C,9.99,2024-01-15 10:10:00
```

Create the topic and run:

```bash
# Create topic
docker exec kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic prod.files.csv.raw \
  --partitions 3 --replication-factor 1 --if-not-exists

# Run producer (venv must be activated)
python scripts/csv_producer.py scripts/sample_data.csv
```

---

## Step 10.2 — Create PostgreSQL consumer: `consumers/postgres_consumer.py`

**What it does:** Reads CDC events from Kafka and writes them to PostgreSQL with proper INSERT/UPDATE/DELETE handling. Unlike the JDBC Sink connector, this Python consumer can do **hard deletes** (physically removes rows for `__op = "d"`).

**Option A — Docker PostgreSQL:**
```python
PG_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "targetdb",
    "user": "kafka_user",
    "password": "kafka_password",
}
```

**Option B — VPS PostgreSQL:**
```python
PG_CONFIG = {
    "host": "62.171.177.208",
    "port": 5432,
    "dbname": "targetdb",
    "user": "kafka_user",
    "password": "YOUR_PASSWORD",
}
```

```python
# consumers/postgres_consumer.py
import json
import psycopg2
import psycopg2.extras
import logging
import signal
import sys
from confluent_kafka import Consumer, KafkaError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

KAFKA_CONFIG = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "custom-postgres-consumer",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
}

# Option A (Docker): use kafka_password
# Option B (VPS):    use your VPS password
PG_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "targetdb",
    "user": "kafka_user",
    "password": "kafka_password",
}

TOPICS = ["prod.mysql.sourcedb.orders"]
BATCH_SIZE = 500

UPSERT_SQL = """
INSERT INTO pipeline.orders
    (id, customer_id, product_id, quantity, amount, status, created_at, updated_at)
VALUES
    (%(id)s, %(customer_id)s, %(product_id)s, %(quantity)s, %(amount)s,
     %(status)s, %(created_at)s, %(updated_at)s)
ON CONFLICT (id) DO UPDATE SET
    customer_id  = EXCLUDED.customer_id,
    product_id   = EXCLUDED.product_id,
    quantity     = EXCLUDED.quantity,
    amount       = EXCLUDED.amount,
    status       = EXCLUDED.status,
    updated_at   = EXCLUDED.updated_at;
"""

DELETE_SQL = "DELETE FROM pipeline.orders WHERE id = %(id)s"

running = True

def signal_handler(sig, frame):
    global running
    logger.info("Shutdown signal received — stopping after current batch")
    running = False

def extract_payload(msg_value):
    """Extract payload from either {schema, payload} or flat format."""
    try:
        data = json.loads(msg_value)
        if "payload" in data:
            return data["payload"]
        return data
    except Exception:
        return None

def process_batch(conn, batch):
    upserts = [r for r in batch if r.get("__op") != "d"]
    deletes = [r for r in batch if r.get("__op") == "d"]
    with conn.cursor() as cur:
        if upserts:
            psycopg2.extras.execute_batch(cur, UPSERT_SQL, upserts, page_size=200)
        if deletes:
            psycopg2.extras.execute_batch(cur, DELETE_SQL, deletes, page_size=200)
    conn.commit()
    logger.info(f"Batch: {len(upserts)} upserts, {len(deletes)} hard deletes")

def main():
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

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
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logger.error(f"Consumer error: {msg.error()}")
                continue

            payload = extract_payload(msg.value())
            if payload:
                batch.append(payload)
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

Run:

```bash
# venv must be activated
python consumers/postgres_consumer.py
# Press Ctrl+C to stop
```

---

## Step 10.3 — Create load test: `scripts/load_test.py`

**What it does:** Inserts 1000 orders into MySQL in batches and measures end-to-end pipeline latency.

```python
# scripts/load_test.py
import mysql.connector
import psycopg2
import time
import random

# Option A (Docker MySQL on port 3307, Docker PostgreSQL on port 5432)
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

# Option B (VPS — change host and password)
# MYSQL_CONFIG = {"host": "62.171.177.208", "port": 3306, ...}
# PG_CONFIG = {"host": "62.171.177.208", "port": 5432, ...}

NUM_ORDERS = 1000
BATCH_SIZE = 100

def run_load_test():
    mysql_conn = mysql.connector.connect(**MYSQL_CONFIG)
    pg_conn = psycopg2.connect(**PG_CONFIG)
    mysql_cur = mysql_conn.cursor()
    pg_cur = pg_conn.cursor()

    print(f"Inserting {NUM_ORDERS} orders into MySQL...")
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
        print(f"  Inserted {batch_start + BATCH_SIZE}/{NUM_ORDERS}")

    insert_done = time.time()
    mysql_cur.execute("SELECT MAX(id) FROM orders")
    max_id = mysql_cur.fetchone()[0]
    print(f"MySQL inserts done in {insert_done - start_time:.2f}s. Waiting for sync...")

    wait_start = time.time()
    while True:
        pg_cur.execute("SELECT MAX(id) FROM pipeline.orders")
        pg_max = pg_cur.fetchone()[0] or 0
        if pg_max >= max_id:
            break
        elapsed = time.time() - wait_start
        if elapsed > 120:
            print("Timeout!")
            break
        print(f"  PG max_id={pg_max} (target={max_id}), {elapsed:.1f}s...")
        time.sleep(1)

    total = time.time() - start_time
    sync_lat = time.time() - insert_done
    print(f"\nResults: total={total:.2f}s, sync_latency={sync_lat:.2f}s, throughput={NUM_ORDERS/total:.0f} events/sec")

    mysql_conn.close()
    pg_conn.close()

if __name__ == "__main__":
    run_load_test()
```

```bash
python scripts/load_test.py
```

---

---

# Phase 11: Monitoring

---

## Step 11.1 — Access Prometheus

Open http://localhost:9090 → **Status** → **Targets**

You should see:
- `kafka-exporter` at `kafka-exporter:9308` → **UP** (consumer lag, topic metrics)
- `kafka-connect` at `kafka-connect:7072` → **DOWN** (expected — Debezium image doesn't expose this port)

The `kafka-exporter` being UP is what matters. The `kafka-connect` DOWN is harmless.

---

## Step 11.2 — Set up Grafana

1. Open http://localhost:3000
2. Login: username `admin`, password `admin` (skip the change password prompt for dev)
3. Click hamburger menu → **Connections** → **Data sources** → **Add data source**
4. Select **Prometheus**
5. URL: `http://prometheus:9090` (use container hostname, not `localhost`)
6. Click **Save & test** → "Data source is working"

---

## Step 11.3 — Import Kafka dashboard

1. Click **+** → **Import**
2. Enter Dashboard ID: **7589** (Kafka Exporter Overview)
3. Click **Load** → select your Prometheus data source → **Import**

Key metrics to watch:

```promql
# Consumer lag — should be 0 at rest
sum by (consumergroup, topic) (kafka_consumergroup_lag)

# Messages per second
rate(kafka_topic_partition_current_offset[1m])

# Consumer group members
kafka_consumergroup_members
```

---

---

# Phase 12: End-to-End Testing

---

## Step 12.1 — Test INSERT

**Option A (Docker MySQL):**

```powershell
docker exec mysql-source mysql -u kafka_user -pkafka_password sourcedb -e "
INSERT INTO orders (customer_id, product_id, quantity, amount, status)
VALUES (1, 201, 3, 149.99, 'PENDING');"
```

**Option B (VPS MySQL — on VPS):**

```bash
mysql -u kafka_user -p'YOUR_PASSWORD' sourcedb -e "
INSERT INTO orders (customer_id, product_id, quantity, amount, status)
VALUES (1, 201, 3, 149.99, 'PENDING');"
```

Wait 3–5 seconds, then verify in PostgreSQL.

---

## Step 12.2 — Test UPDATE

```bash
# Option A (Docker MySQL)
docker exec mysql-source mysql -u kafka_user -pkafka_password sourcedb -e \
  "UPDATE orders SET status = 'COMPLETED', product_id = 999 WHERE id = 1;"

# Option B (VPS MySQL — on VPS)
mysql -u kafka_user -p'YOUR_PASSWORD' sourcedb -e \
  "UPDATE orders SET status = 'COMPLETED', product_id = 999 WHERE id = 1;"
```

> **MySQL no-op UPDATE rule:** MySQL does NOT generate a binlog event if the UPDATE doesn't actually change any column value. Always update to a genuinely different value, otherwise no CDC event is produced and nothing appears in Kafka.

---

## Step 12.3 — Watch Kafka topic in real-time

**Windows PowerShell:**

```powershell
docker exec kafka kafka-console-consumer `
  --bootstrap-server localhost:9092 `
  --topic prod.mysql.sourcedb.orders `
  --from-beginning `
  --timeout-ms 30000
```

**Linux / macOS:**

```bash
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic prod.mysql.sourcedb.orders \
  --from-beginning \
  --timeout-ms 30000
```

Each message will be a large JSON with `"schema"` and `"payload"` sections. The `"payload"."__op"` field shows the operation: `"c"` = create, `"u"` = update, `"d"` = delete.

---

## Step 12.4 — Monitor consumer lag

```bash
# Windows PowerShell
docker exec kafka kafka-consumer-groups `
  --bootstrap-server localhost:9092 `
  --describe `
  --group connect-postgres-sink

# Linux / macOS
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe \
  --group connect-postgres-sink
```

`LAG` should be 0 during idle. It increases during bulk inserts and returns to 0.

---

## Step 12.5 — Check DLQ for failed messages

```bash
# Windows PowerShell
docker exec kafka kafka-console-consumer `
  --bootstrap-server localhost:9092 `
  --topic prod.dlq.errors `
  --from-beginning `
  --max-messages 10 `
  --timeout-ms 5000

# Linux / macOS
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic prod.dlq.errors \
  --from-beginning \
  --max-messages 10 \
  --timeout-ms 5000
```

A healthy pipeline's DLQ should be empty. Messages here indicate records that failed processing and need investigation.

---

---

# Scenario A: MySQL → PostgreSQL

This is the default scenario covered in Phases 1–12. The full setup is documented above.

**Summary:**
- Source: `io.debezium.connector.mysql.MySqlConnector`
- Sink: `io.debezium.connector.jdbc.JdbcSinkConnector`
- MySQL binlog → Kafka → PostgreSQL UPSERT
- Soft deletes: MySQL DELETE → `__deleted = true` in PostgreSQL

See [connectors/source/mysql-cdc-source.json](../connectors/source/mysql-cdc-source.json) and [connectors/sink/postgres-sink.json](../connectors/sink/postgres-sink.json).

---

---

# Scenario B: PostgreSQL → PostgreSQL

**Use case:** Replicate tables from one PostgreSQL database to another. Example: production PostgreSQL (VPS) → analytics PostgreSQL (Docker or another server).

**How it works:** Debezium reads PostgreSQL's Write-Ahead Log (WAL) via logical replication. Every INSERT, UPDATE, DELETE is captured as a change event and published to Kafka. The JDBC Sink connector then writes those events to the target PostgreSQL.

---

## B.1 — Source PostgreSQL pre-requisites

**Why:** PostgreSQL WAL replication requires `wal_level = logical`. Default is `replica`, which does not expose row-level change data.

**On the SOURCE PostgreSQL (VPS — 62.171.177.208):**

```bash
# Edit postgresql.conf
sudo vi /var/lib/pgsql/17/data/postgresql.conf
```

Set:

```ini
wal_level = logical
max_wal_senders = 10
max_replication_slots = 10
```

```bash
sudo systemctl restart postgresql-17
```

**Create replication user and publication:**

```bash
sudo -u postgres psql -d sourcedb
```

```sql
-- Create a replication user (or grant to existing kafka_user)
ALTER USER kafka_user REPLICATION;

-- Create a publication — tells PostgreSQL WHICH tables to replicate
-- Option 1: all tables in a schema
CREATE PUBLICATION kafka_pub FOR ALL TABLES;

-- Option 2: specific tables only
CREATE PUBLICATION kafka_pub FOR TABLE public.orders, public.customers;

-- Verify
SELECT * FROM pg_publication;
```

---

## B.2 — Create Kafka topics for PostgreSQL source

The topic naming for PostgreSQL CDC is: `{topic.prefix}.{schema}.{table}`

For `public.orders` with prefix `prod.pg`, the topic is `prod.pg.public.orders`.

**Windows PowerShell:**

```powershell
docker exec kafka kafka-topics --create --bootstrap-server localhost:9092 --topic prod.pg.public.orders --partitions 3 --replication-factor 1 --if-not-exists

docker exec kafka kafka-topics --create --bootstrap-server localhost:9092 --topic prod.pg.public.customers --partitions 3 --replication-factor 1 --if-not-exists

docker exec kafka kafka-topics --create --bootstrap-server localhost:9092 --topic prod.dlq.pg.errors --partitions 1 --replication-factor 1 --config retention.ms=-1 --if-not-exists
```

**Linux / macOS:**

```bash
docker exec kafka kafka-topics --create --bootstrap-server localhost:9092 --topic prod.pg.public.orders --partitions 3 --replication-factor 1 --if-not-exists
docker exec kafka kafka-topics --create --bootstrap-server localhost:9092 --topic prod.pg.public.customers --partitions 3 --replication-factor 1 --if-not-exists
docker exec kafka kafka-topics --create --bootstrap-server localhost:9092 --topic prod.dlq.pg.errors --partitions 1 --replication-factor 1 --config retention.ms=-1 --if-not-exists
```

---

## B.3 — Create source connector: `connectors/source/postgres-cdc-source.json`

**Key differences from MySQL source:**
- `connector.class`: `io.debezium.connector.postgresql.PostgresConnector`
- `plugin.name`: `pgoutput` — the built-in PostgreSQL logical decoding plugin (no extra install needed for PG 10+)
- `slot.name`: unique name for the replication slot Debezium creates
- `publication.name`: must match the publication you created in B.1
- No `database.server.id` — that's MySQL-only

### Option A — Source is Docker PostgreSQL

```json
{
  "name": "pg-cdc-source",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "tasks.max": "1",

    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "kafka_user",
    "database.password": "kafka_password",
    "database.dbname": "sourcedb",
    "database.server.name": "prod.pg",

    "topic.prefix": "prod.pg",
    "schema.include.list": "public",
    "table.include.list": "public.orders,public.customers",

    "plugin.name": "pgoutput",
    "slot.name": "debezium_slot",
    "publication.name": "kafka_pub",

    "snapshot.mode": "initial",

    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "true",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "true",

    "transforms": "unwrap,addMetadata",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.add.fields": "op,ts_ms,source.db,source.schema,source.table",
    "transforms.unwrap.delete.handling.mode": "rewrite",
    "transforms.unwrap.drop.tombstones": "true",

    "transforms.addMetadata.type": "org.apache.kafka.connect.transforms.InsertField$Value",
    "transforms.addMetadata.static.field": "_pipeline_version",
    "transforms.addMetadata.static.value": "1.0",

    "errors.tolerance": "all",
    "errors.log.enable": "true",
    "errors.deadletterqueue.topic.name": "prod.dlq.pg.errors",
    "errors.deadletterqueue.topic.replication.factor": "1",

    "heartbeat.interval.ms": "10000"
  }
}
```

### Option B — Source is VPS PostgreSQL (62.171.177.208)

```json
{
  "name": "pg-cdc-source",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "tasks.max": "1",

    "database.hostname": "62.171.177.208",
    "database.port": "5432",
    "database.user": "kafka_user",
    "database.password": "YOUR_PASSWORD",
    "database.dbname": "sourcedb",
    "database.server.name": "prod.pg",

    "topic.prefix": "prod.pg",
    "schema.include.list": "public",
    "table.include.list": "public.orders,public.customers",

    "plugin.name": "pgoutput",
    "slot.name": "debezium_slot",
    "publication.name": "kafka_pub",

    "snapshot.mode": "initial",

    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "true",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "true",

    "transforms": "unwrap,addMetadata",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.add.fields": "op,ts_ms,source.db,source.schema,source.table",
    "transforms.unwrap.delete.handling.mode": "rewrite",
    "transforms.unwrap.drop.tombstones": "true",

    "transforms.addMetadata.type": "org.apache.kafka.connect.transforms.InsertField$Value",
    "transforms.addMetadata.static.field": "_pipeline_version",
    "transforms.addMetadata.static.value": "1.0",

    "errors.tolerance": "all",
    "errors.log.enable": "true",
    "errors.deadletterqueue.topic.name": "prod.dlq.pg.errors",
    "errors.deadletterqueue.topic.replication.factor": "1",

    "heartbeat.interval.ms": "10000"
  }
}
```

Save as: `connectors/source/postgres-cdc-source.json`

---

## B.4 — Create sink connector: `connectors/sink/postgres-pg-sink.json`

**What changes from the MySQL sink:** Only the `topics`, `connection.url`, and the `RegexRouter` pattern (because topic prefix is now `prod.pg.public` instead of `prod.mysql.sourcedb`).

### Option A — Target is Docker PostgreSQL

```json
{
  "name": "postgres-pg-sink",
  "config": {
    "connector.class": "io.debezium.connector.jdbc.JdbcSinkConnector",
    "tasks.max": "2",

    "connection.url": "jdbc:postgresql://postgres:5432/targetdb",
    "connection.username": "kafka_user",
    "connection.password": "kafka_password",

    "topics": "prod.pg.public.orders,prod.pg.public.customers",

    "insert.mode": "upsert",
    "primary.key.mode": "record_value",
    "primary.key.fields": "id",

    "schema.evolution": "basic",
    "table.name.format": "pipeline.${topic}",

    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "true",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "true",

    "transforms": "router",
    "transforms.router.type": "org.apache.kafka.connect.transforms.RegexRouter",
    "transforms.router.regex": "prod\\.pg\\.public\\.(.*)",
    "transforms.router.replacement": "$1",

    "batch.size": "3000",
    "max.retries": "10",
    "retry.backoff.ms": "3000",

    "errors.tolerance": "all",
    "errors.log.enable": "true",
    "errors.deadletterqueue.topic.name": "prod.dlq.pg.errors"
  }
}
```

### Option B — Target is VPS PostgreSQL (62.171.177.208)

Change only:

```json
"connection.url": "jdbc:postgresql://62.171.177.208:5432/targetdb",
"connection.username": "kafka_user",
"connection.password": "YOUR_PASSWORD",
```

Save as: `connectors/sink/postgres-pg-sink.json`

---

## B.5 — Register and verify

**Windows PowerShell:**

```powershell
# Register source
curl.exe -X POST http://localhost:8083/connectors `
  -H "Content-Type: application/json" `
  -d "@connectors/source/postgres-cdc-source.json"

# Wait 20 seconds for snapshot, then register sink
curl.exe -X POST http://localhost:8083/connectors `
  -H "Content-Type: application/json" `
  -d "@connectors/sink/postgres-pg-sink.json"

# Verify both running
curl.exe http://localhost:8083/connectors/pg-cdc-source/status
curl.exe http://localhost:8083/connectors/postgres-pg-sink/status
```

**Linux / macOS:**

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @connectors/source/postgres-cdc-source.json

sleep 20

curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @connectors/sink/postgres-pg-sink.json

curl http://localhost:8083/connectors/pg-cdc-source/status
curl http://localhost:8083/connectors/postgres-pg-sink/status
```

---

## B.6 — Common errors (PostgreSQL → PostgreSQL)

**Error: `replication slot "debezium_slot" already exists`**
- Cause: A previous connector run created the slot and it wasn't cleaned up.
- Fix:
  ```sql
  -- Run on SOURCE PostgreSQL
  SELECT pg_drop_replication_slot('debezium_slot');
  ```
  Then re-register the source connector.

**Error: `publication "kafka_pub" does not exist`**
- Fix: Create the publication on source PostgreSQL (Step B.1).

**Error: `must be superuser or replication role`**
- Fix: Grant replication role to `kafka_user`:
  ```sql
  ALTER USER kafka_user REPLICATION;
  ```

**Replication slot fills up disk (production concern)**
- If the Debezium connector stops for a long time, WAL accumulates for the slot and can fill your disk.
- Monitor: `SELECT slot_name, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS lag FROM pg_replication_slots;`
- If the connector will be down for a long time, drop the slot: `SELECT pg_drop_replication_slot('debezium_slot');`

---

---

# Scenario C: Flat Files (CSV) → PostgreSQL

**Use case:** Load CSV/flat file data into PostgreSQL via Kafka. Example: daily export files from a legacy system, uploaded data files, batch imports.

**How it works:** A Python producer script reads each CSV row and publishes it to a Kafka topic as a JSON message. A separate Python consumer (or JDBC sink) reads from that topic and writes to PostgreSQL.

---

## C.1 — Create Kafka topic for file data

**Windows PowerShell:**

```powershell
docker exec kafka kafka-topics --create `
  --bootstrap-server localhost:9092 `
  --topic prod.files.csv.orders `
  --partitions 3 `
  --replication-factor 1 `
  --if-not-exists
```

**Linux / macOS:**

```bash
docker exec kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic prod.files.csv.orders \
  --partitions 3 \
  --replication-factor 1 \
  --if-not-exists
```

---

## C.2 — CSV format (expected columns)

Your CSV file should have a header row. Example `data/orders_export.csv`:

```csv
id,customer_id,product_id,quantity,amount,status,created_at
1001,5,201,2,49.99,COMPLETED,2024-03-01 10:00:00
1002,6,202,1,99.00,PENDING,2024-03-01 10:05:00
1003,7,203,3,29.99,PROCESSING,2024-03-01 10:10:00
```

---

## C.3 — Create CSV producer: `scripts/csv_to_kafka.py`

**What it does:** Reads each row of the CSV and publishes it as a JSON message to Kafka. Uses the `id` column as the Kafka message key so that upserts work correctly in the sink.

```python
# scripts/csv_to_kafka.py
import csv
import json
import sys
from pathlib import Path
from confluent_kafka import Producer

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "prod.files.csv.orders"

def delivery_report(err, msg):
    if err:
        print(f"[ERROR] Failed to deliver row {msg.key()}: {err}")

def produce_csv(file_path: str):
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    file_name = Path(file_path).name
    row_num = 0

    with open(file_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row_num, row in enumerate(reader, start=1):
            # Add pipeline metadata
            row["_source_file"] = file_name
            row["_row_number"] = str(row_num)

            producer.produce(
                topic=TOPIC,
                key=row.get("id", str(row_num)),   # use id as key for deduplication
                value=json.dumps(row),
                on_delivery=delivery_report
            )

            # Flush every 1000 rows to avoid buffer overflow
            if row_num % 1000 == 0:
                producer.poll(0)
                print(f"  Published {row_num} rows...")

    producer.flush()
    print(f"Done. Published {row_num} rows from '{file_name}' → topic '{TOPIC}'")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python scripts/csv_to_kafka.py <path/to/file.csv>")
        sys.exit(1)
    produce_csv(sys.argv[1])
```

Run:

```bash
# venv must be activated
python scripts/csv_to_kafka.py data/orders_export.csv
```

---

## C.4 — Create PostgreSQL sink connector for CSV topic

**Why use a connector instead of Python consumer?** The JDBC Sink connector handles batching, retries, and schema evolution automatically. For CSV data we use JSON format (no embedded schema), so we use `schemas.enable: false` and rely on `insert.mode: insert`.

> **Note:** For CSV → PostgreSQL via JDBC Sink, the target table must already exist with the correct columns. The connector cannot infer schema from plain JSON (no embedded schema info). Create the table first.

**Create target table on PostgreSQL:**

```sql
-- Run on target PostgreSQL (Docker or VPS)
CREATE TABLE IF NOT EXISTS pipeline.csv_orders (
    id          BIGINT PRIMARY KEY,
    customer_id BIGINT,
    product_id  BIGINT,
    quantity    INT,
    amount      NUMERIC(10,2),
    status      VARCHAR(50),
    created_at  TIMESTAMP,
    _source_file VARCHAR(255),
    _row_number  INT,
    _loaded_at   TIMESTAMP DEFAULT NOW()
);
GRANT ALL PRIVILEGES ON pipeline.csv_orders TO kafka_user;
```

**Create connector: `connectors/sink/csv-postgres-sink.json`**

### Option A — Docker PostgreSQL

```json
{
  "name": "csv-postgres-sink",
  "config": {
    "connector.class": "io.debezium.connector.jdbc.JdbcSinkConnector",
    "tasks.max": "2",

    "connection.url": "jdbc:postgresql://postgres:5432/targetdb",
    "connection.username": "kafka_user",
    "connection.password": "kafka_password",

    "topics": "prod.files.csv.orders",

    "insert.mode": "upsert",
    "primary.key.mode": "record_value",
    "primary.key.fields": "id",

    "schema.evolution": "basic",
    "table.name.format": "pipeline.csv_orders",

    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",

    "batch.size": "3000",
    "max.retries": "10",
    "retry.backoff.ms": "3000",

    "errors.tolerance": "all",
    "errors.log.enable": "true",
    "errors.deadletterqueue.topic.name": "prod.dlq.errors"
  }
}
```

### Option B — VPS PostgreSQL

```json
{
  "name": "csv-postgres-sink",
  "config": {
    "connector.class": "io.debezium.connector.jdbc.JdbcSinkConnector",
    "tasks.max": "2",

    "connection.url": "jdbc:postgresql://62.171.177.208:5432/targetdb",
    "connection.username": "kafka_user",
    "connection.password": "YOUR_PASSWORD",

    "topics": "prod.files.csv.orders",

    "insert.mode": "upsert",
    "primary.key.mode": "record_value",
    "primary.key.fields": "id",

    "schema.evolution": "basic",
    "table.name.format": "pipeline.csv_orders",

    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",

    "batch.size": "3000",
    "max.retries": "10",
    "retry.backoff.ms": "3000",

    "errors.tolerance": "all",
    "errors.log.enable": "true",
    "errors.deadletterqueue.topic.name": "prod.dlq.errors"
  }
}
```

---

## C.5 — Register the CSV sink connector

**Windows PowerShell:**

```powershell
curl.exe -X POST http://localhost:8083/connectors `
  -H "Content-Type: application/json" `
  -d "@connectors/sink/csv-postgres-sink.json"

curl.exe http://localhost:8083/connectors/csv-postgres-sink/status
```

**Linux / macOS:**

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @connectors/sink/csv-postgres-sink.json

curl http://localhost:8083/connectors/csv-postgres-sink/status
```

---

## C.6 — Full workflow: Load a CSV file end-to-end

```bash
# Step 1: Place your CSV in the data/ folder
mkdir data
# Copy your CSV file to data/orders_export.csv

# Step 2: Publish CSV rows to Kafka (venv activated)
python scripts/csv_to_kafka.py data/orders_export.csv

# Step 3: Wait 5 seconds, then verify rows in PostgreSQL
# Option A (Docker)
docker exec postgres-target psql -U kafka_user -d targetdb \
  -c "SELECT COUNT(*) FROM pipeline.csv_orders;"

# Option B (VPS — run on VPS)
psql -U kafka_user -d targetdb -c "SELECT COUNT(*) FROM pipeline.csv_orders;"
```

---

## C.7 — Python consumer alternative (hard deletes + custom logic)

If you need custom transformation logic (data cleaning, type conversion, conditional inserts), use the Python consumer instead of the JDBC Sink connector:

```python
# consumers/csv_consumer.py
import json
import psycopg2
import psycopg2.extras
import logging
from confluent_kafka import Consumer, KafkaError
from decimal import Decimal

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

KAFKA_CONFIG = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "csv-pg-consumer",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
}

# Option A (Docker): kafka_password
# Option B (VPS):    your VPS password
PG_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "targetdb",
    "user": "kafka_user",
    "password": "kafka_password",
}

TOPIC = "prod.files.csv.orders"

UPSERT_SQL = """
INSERT INTO pipeline.csv_orders
    (id, customer_id, product_id, quantity, amount, status, created_at, _source_file)
VALUES
    (%(id)s, %(customer_id)s, %(product_id)s, %(quantity)s, %(amount)s,
     %(status)s, %(created_at)s, %(_source_file)s)
ON CONFLICT (id) DO UPDATE SET
    customer_id  = EXCLUDED.customer_id,
    product_id   = EXCLUDED.product_id,
    quantity     = EXCLUDED.quantity,
    amount       = EXCLUDED.amount,
    status       = EXCLUDED.status;
"""

def clean_row(row: dict) -> dict:
    """Convert CSV string values to proper Python types."""
    return {
        "id":           int(row.get("id", 0)),
        "customer_id":  int(row.get("customer_id", 0)),
        "product_id":   int(row.get("product_id", 0)),
        "quantity":     int(row.get("quantity", 0)),
        "amount":       Decimal(row.get("amount", "0")),
        "status":       row.get("status", "PENDING"),
        "created_at":   row.get("created_at") or None,
        "_source_file": row.get("_source_file", ""),
    }

def main():
    consumer = Consumer(KAFKA_CONFIG)
    consumer.subscribe([TOPIC])
    conn = psycopg2.connect(**PG_CONFIG)
    batch = []
    msgs = []

    try:
        while True:
            msg = consumer.poll(timeout=2.0)
            if msg is None:
                if batch:
                    with conn.cursor() as cur:
                        psycopg2.extras.execute_batch(cur, UPSERT_SQL, batch)
                    conn.commit()
                    consumer.commit(offsets=msgs)
                    logger.info(f"Committed {len(batch)} rows")
                    batch.clear()
                    msgs.clear()
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logger.error(f"Error: {msg.error()}")
                continue

            row = json.loads(msg.value())
            batch.append(clean_row(row))
            msgs.append(msg)

            if len(batch) >= 500:
                with conn.cursor() as cur:
                    psycopg2.extras.execute_batch(cur, UPSERT_SQL, batch)
                conn.commit()
                consumer.commit(offsets=msgs)
                logger.info(f"Committed {len(batch)} rows")
                batch.clear()
                msgs.clear()

    except KeyboardInterrupt:
        logger.info("Stopping...")
    finally:
        if batch:
            with conn.cursor() as cur:
                psycopg2.extras.execute_batch(cur, UPSERT_SQL, batch)
            conn.commit()
        consumer.close()
        conn.close()

if __name__ == "__main__":
    main()
```

Run:

```bash
python consumers/csv_consumer.py
# Ctrl+C to stop
```

---

## C.8 — Common errors (CSV → PostgreSQL)

**Error: column `id` does not exist / wrong column type**
- CSV columns are all strings. Create the PostgreSQL table first with correct types (Step C.4), or use the Python consumer with `clean_row()` (Step C.7) to cast types.

**Error: `duplicate key value violates unique constraint`**
- Ensure `insert.mode: upsert` and `primary.key.fields: id` are set in the connector.

**CSV has no `id` column**
- Use a generated key: change the producer to use `key=str(uuid.uuid4())` and set `primary.key.mode: kafka` in the sink connector.

**Large CSV files (100k+ rows) are slow**
- Increase `batch.size` in the sink connector to `10000`
- Or use the Python consumer with larger page_size in `execute_batch`

---

---

## Scenario Comparison

| Scenario | Source Connector | Sink | Key Requirement |
|----------|-----------------|------|----------------|
| MySQL → PostgreSQL | `MySqlConnector` | JDBC Sink | MySQL binlog ROW format |
| PostgreSQL → PostgreSQL | `PostgresConnector` | JDBC Sink | `wal_level = logical` + publication |
| CSV → PostgreSQL | Python producer | JDBC Sink or Python consumer | Target table must exist |

---

---

# Quick Reference

## All connector REST API commands

```bash
# List all connectors
curl.exe http://localhost:8083/connectors

# Check source status
curl.exe http://localhost:8083/connectors/mysql-cdc-source/status

# Check sink status
curl.exe http://localhost:8083/connectors/postgres-sink/status

# Register source
curl.exe -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d "@connectors/source/mysql-cdc-source.json"

# Register sink
curl.exe -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d "@connectors/sink/postgres-sink.json"

# Delete source
curl.exe -X DELETE http://localhost:8083/connectors/mysql-cdc-source

# Delete sink
curl.exe -X DELETE http://localhost:8083/connectors/postgres-sink

# Restart a failed connector
curl.exe -X POST "http://localhost:8083/connectors/postgres-sink/restart?includeTasks=true"
```

> On **Linux/macOS**, replace `curl.exe` with `curl`.

## Docker Compose commands

```bash
docker compose up -d           # Start all containers (background)
docker compose down            # Stop all containers (keep volumes)
docker compose down -v         # Stop + delete all data volumes (clean slate)
docker compose ps              # List container status
docker compose restart kafka-connect   # Restart one service
docker logs kafka-connect --tail 50    # View recent logs
```

## Common diagnostic checks

```bash
# Is Kafka ready?
curl.exe http://localhost:8083/

# Are connectors running?
curl.exe http://localhost:8083/connectors

# How many messages in a topic?
docker exec kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic prod.mysql.sourcedb.orders

# Consumer group lag
docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group connect-postgres-sink

# Check VPS connectivity
Test-NetConnection -ComputerName 62.171.177.208 -Port 3306
Test-NetConnection -ComputerName 62.171.177.208 -Port 5432
```
