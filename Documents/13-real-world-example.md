# Real-World Example: MySQL to Kafka to PostgreSQL (CDC Pipeline)

## Overview

End-to-end implementation of a production-grade CDC pipeline:
- **Source**: MySQL 8.0 (`sourcedb.orders`, `sourcedb.customers`)
- **Transport**: Apache Kafka with Schema Registry
- **Sink**: PostgreSQL 16 (`targetdb.pipeline.orders`, `targetdb.pipeline.customers`)
- **Scenario**: E-commerce order replication with real-time sync

**Order of operations (do NOT skip steps):**

```
1. docker compose up -d
2. Wait for kafka-connect → healthy  (~2-3 min)
3. Create all Kafka topics
4. Register source connector
5. Wait for snapshot to complete (~30 sec)
6. Register sink connector
7. Verify data in PostgreSQL
```

---

## Step 1: Start the Environment

**Why:** All services (Kafka, Connect, MySQL, PostgreSQL, monitoring) must be up before topics can be created or connectors registered.

**What happens:** Docker Compose starts 9 containers in dependency order. `kafka-connect` waits for `kafka` to be healthy before starting.

```bash
# Create project directory (all platforms — docker exec commands work the same)
mkdir kafka-pipeline
cd kafka-pipeline

# Create subdirectories
# Linux / macOS:
mkdir -p connectors/source connectors/sink scripts monitoring/grafana/dashboards config consumers

# Windows PowerShell:
# mkdir connectors\source, connectors\sink, scripts, monitoring\grafana\dashboards, config, consumers
```

Create the required files (`scripts/mysql-init.sql`, `scripts/postgres-init.sql`, `monitoring/prometheus.yml`, `docker-compose.yml`, and both connector JSON files) as described in `03-environment-setup.md`, `04-data-ingestion.md`, and `06-data-sink.md`.

Then start everything:

```bash
docker compose up -d
```

Watch startup progress:

```bash
docker compose ps
```

Wait until `kafka-connect` shows `Up (healthy)`. This takes approximately 2–3 minutes.

---

## Step 2: Verify Source Database

**Why:** Confirm MySQL is up, binlog is enabled, and `kafka_user` has the required CDC permissions before registering the source connector.

```bash
# Connect to MySQL
docker exec -it mysql-source mysql -u kafka_user -pkafka_password sourcedb
```

Inside MySQL:

```sql
-- Verify CDC user permissions
SHOW GRANTS FOR 'kafka_user'@'%';
-- Must include: REPLICATION SLAVE, REPLICATION CLIENT

-- Verify binlog is enabled
SHOW VARIABLES LIKE 'log_bin';           -- should be ON
SHOW VARIABLES LIKE 'binlog_format';     -- should be ROW
SHOW VARIABLES LIKE 'binlog_row_image';  -- should be FULL

-- Check current data
SELECT * FROM orders LIMIT 5;
SELECT * FROM customers LIMIT 5;
EXIT;
```

Expected data (from mysql-init.sql):

```
+----+-------------+------------+----------+--------+-----------+
| id | customer_id | product_id | quantity | amount | status    |
+----+-------------+------------+----------+--------+-----------+
|  1 |           1 |        101 |        2 |  49.99 | COMPLETED |
|  2 |           2 |        102 |        1 |  99.00 | PENDING   |
+----+-------------+------------+----------+--------+-----------+
```

---

## Step 3: Create Kafka Topics

**Why:** Kafka is configured with `KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"`. Topics must exist before connectors are registered, otherwise connectors fail silently or crash.

**What happens:** Topics are created with explicit partition counts and retention settings. The heartbeat and schema-change topics are required by Debezium internally.

**Windows PowerShell:**

```powershell
# CDC source topics
docker exec kafka kafka-topics --create --bootstrap-server localhost:9092 --topic prod.mysql.sourcedb.orders --partitions 3 --replication-factor 1 --config retention.ms=604800000 --if-not-exists

docker exec kafka kafka-topics --create --bootstrap-server localhost:9092 --topic prod.mysql.sourcedb.customers --partitions 3 --replication-factor 1 --config retention.ms=604800000 --if-not-exists

# Dead Letter Queue — retains forever for investigation
docker exec kafka kafka-topics --create --bootstrap-server localhost:9092 --topic prod.dlq.errors --partitions 1 --replication-factor 1 --config retention.ms=-1 --if-not-exists

# Kafka Connect internal topics
docker exec kafka kafka-topics --create --bootstrap-server localhost:9092 --topic _connect-configs --partitions 1 --replication-factor 1 --if-not-exists

docker exec kafka kafka-topics --create --bootstrap-server localhost:9092 --topic _connect-offsets --partitions 25 --replication-factor 1 --if-not-exists

docker exec kafka kafka-topics --create --bootstrap-server localhost:9092 --topic _connect-status --partitions 5 --replication-factor 1 --if-not-exists

# Required by Debezium heartbeat
docker exec kafka kafka-topics --create --bootstrap-server localhost:9092 --topic __debezium-heartbeat.prod.mysql --partitions 1 --replication-factor 1 --if-not-exists

# Required by Debezium schema changes (include.schema.changes: true)
docker exec kafka kafka-topics --create --bootstrap-server localhost:9092 --topic prod.mysql --partitions 1 --replication-factor 1 --if-not-exists
```

**Linux (AlmaLinux 9) / macOS (M1/M2/M3):**

```bash
for TOPIC in \
  "prod.mysql.sourcedb.orders:3" \
  "prod.mysql.sourcedb.customers:3" \
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

Verify topics were created:

```bash
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092
```

Or open Kafka UI: http://localhost:8090 → Topics

---

## Step 4: Deploy Debezium MySQL CDC Source Connector

**Why:** The source connector registers as a MySQL replica, reads the binlog, and publishes CDC events to Kafka topics.

**What happens:** On registration, Debezium immediately starts a snapshot of all existing rows in `sourcedb.orders` and `sourcedb.customers`. After the snapshot completes (~30 seconds for small tables), it streams all new changes in real-time.

Ensure `connectors/source/mysql-cdc-source.json` exists with the correct content (see `04-data-ingestion.md`). The connector must use `kafka_user` / `kafka_password` for Docker setup (Option A).

**Linux (AlmaLinux 9) / macOS (M1/M2/M3):**

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @connectors/source/mysql-cdc-source.json
```

**Windows PowerShell:**

```powershell
curl.exe -X POST http://localhost:8083/connectors `
  -H "Content-Type: application/json" `
  -d "@connectors/source/mysql-cdc-source.json"
```

Check status (wait ~10 seconds first):

**Linux (AlmaLinux 9) / macOS (M1/M2/M3):**

```bash
curl -s http://localhost:8083/connectors/mysql-cdc-source/status | python3 -m json.tool
```

**Windows PowerShell:**

```powershell
curl.exe http://localhost:8083/connectors/mysql-cdc-source/status
```

Expected response:

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

---

## Step 5: Verify Data in Kafka

**Why:** Confirm the initial snapshot completed successfully before deploying the sink connector.

**What happens:** After the source connector starts, Debezium captures all existing rows in a snapshot. For 2 rows this takes seconds. For millions of rows it can take minutes.

Check message count:

```bash
docker exec kafka kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 \
  --topic prod.mysql.sourcedb.orders
```

You should see non-zero offsets.

View actual message content:

**Windows PowerShell:**

```powershell
docker exec kafka kafka-console-consumer `
  --bootstrap-server localhost:9092 `
  --topic prod.mysql.sourcedb.orders `
  --from-beginning `
  --max-messages 3 `
  --timeout-ms 10000
```

**Linux (AlmaLinux 9) / macOS (M1/M2/M3):**

```bash
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic prod.mysql.sourcedb.orders \
  --from-beginning \
  --max-messages 3 \
  --timeout-ms 10000
```

Each message should be a large JSON object with both `"schema"` and `"payload"` fields. If you see raw values without schema, check that `schemas.enable: "true"` is set in the connector config.

Open Kafka UI: http://localhost:8090 → Topics → `prod.mysql.sourcedb.orders`

---

## Step 6: Deploy PostgreSQL Sink Connector

**Why:** The sink connector reads from the Kafka topics and writes to PostgreSQL. It must be deployed AFTER the snapshot is complete to avoid schema issues.

**What happens:** The sink reads from `prod.mysql.sourcedb.orders` and `prod.mysql.sourcedb.customers`, uses RegexRouter to strip the topic prefix, and creates `pipeline.orders` and `pipeline.customers` automatically via `schema.evolution: basic`. It then UPSERTs all snapshot rows.

Ensure `connectors/sink/postgres-sink.json` exists with the correct content (see `06-data-sink.md`).

**Linux (AlmaLinux 9) / macOS (M1/M2/M3):**

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @connectors/sink/postgres-sink.json
```

**Windows PowerShell:**

```powershell
curl.exe -X POST http://localhost:8083/connectors `
  -H "Content-Type: application/json" `
  -d "@connectors/sink/postgres-sink.json"
```

Check status (wait ~10 seconds):

**Linux (AlmaLinux 9) / macOS (M1/M2/M3):**

```bash
curl -s http://localhost:8083/connectors/postgres-sink/status | python3 -m json.tool
```

**Windows PowerShell:**

```powershell
curl.exe http://localhost:8083/connectors/postgres-sink/status
```

---

## Step 7: Verify Data in PostgreSQL

**Why:** Confirm the data landed correctly in the target database.

```bash
# Connect to PostgreSQL target
docker exec -it postgres-target psql -U kafka_user -d targetdb
```

Inside psql:

```sql
-- Check data was synced
SELECT COUNT(*) FROM pipeline.orders;
SELECT COUNT(*) FROM pipeline.customers;

SELECT id, customer_id, amount, status
FROM pipeline.orders
ORDER BY id;

\q
```

---

## Step 8: Test Real-Time Sync

### Insert new order in MySQL

```bash
docker exec mysql-source mysql -u kafka_user -pkafka_password sourcedb -e "
INSERT INTO orders (customer_id, product_id, quantity, amount, status)
VALUES (1, 201, 3, 149.99, 'PENDING');
"

echo "Inserted new order. Waiting for replication..."
sleep 3

# Verify it appeared in PostgreSQL
docker exec postgres-target psql -U kafka_user -d targetdb -c "
SELECT id, customer_id, amount, status
FROM pipeline.orders
ORDER BY id DESC
LIMIT 3;
"
```

### Update order status

```bash
docker exec mysql-source mysql -u kafka_user -pkafka_password sourcedb -e "
UPDATE orders SET status = 'PROCESSING' WHERE id = (SELECT MAX(id) FROM orders);
"

sleep 2

docker exec postgres-target psql -U kafka_user -d targetdb -c "
SELECT id, status
FROM pipeline.orders
ORDER BY id DESC
LIMIT 3;
"
```

### Delete an order

```bash
docker exec mysql-source mysql -u kafka_user -pkafka_password sourcedb -e "
DELETE FROM orders WHERE id = 1;
"

sleep 2

# Verify row was soft-deleted in PostgreSQL (rewrite mode — row stays with __deleted=true)
docker exec postgres-target psql -U kafka_user -d targetdb -c "
SELECT COUNT(*) FROM pipeline.orders WHERE id = 1;
"
```

> **Note:** With `delete.handling.mode: rewrite` (configured in the source connector), deleted rows are not physically removed from PostgreSQL. Instead, they are updated with `__deleted: true`. This preserves audit history. If you want physical DELETEs, change the source connector to `delete.handling.mode: tombstone` and set `drop.tombstones: false` — but ensure the sink supports delete mode.

---

## Step 9: Run Load Test

**Why:** Measure pipeline latency and throughput under realistic load conditions.

**What happens:** The script inserts 10,000 orders in batches of 100, then polls PostgreSQL until it sees the last inserted ID — measuring total pipeline sync time.

**Note:** MySQL is accessed on port 3307 (Docker host port mapping — the container runs on 3306 internally).

```python
# scripts/load_test.py
"""Inserts 10,000 orders into MySQL and measures pipeline latency."""
import mysql.connector
import psycopg2
import time
import random

MYSQL_CONFIG = {
    "host": "localhost", "port": 3307,
    "database": "sourcedb", "user": "kafka_user", "password": "kafka_password"
}
PG_CONFIG = {
    "host": "localhost", "port": 5432,
    "dbname": "targetdb", "user": "kafka_user", "password": "kafka_password"
}

NUM_ORDERS = 10_000
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
            (random.randint(1, 100), random.randint(100, 999),
             random.randint(1, 10), round(random.uniform(10, 500), 2),
             random.choice(['PENDING', 'PROCESSING', 'COMPLETED']))
            for _ in range(BATCH_SIZE)
        ]
        mysql_cur.executemany(
            "INSERT INTO orders (customer_id, product_id, quantity, amount, status) VALUES (%s,%s,%s,%s,%s)",
            batch
        )
        mysql_conn.commit()

        if (batch_start // BATCH_SIZE) % 10 == 0:
            print(f"  Inserted {batch_start + BATCH_SIZE} orders...")

    insert_time = time.time() - start_time
    print(f"MySQL inserts done in {insert_time:.2f}s ({NUM_ORDERS/insert_time:.0f} rows/s)")

    # Wait for pipeline to sync
    print("Waiting for pipeline to catch up...")
    mysql_cur.execute("SELECT MAX(id) FROM orders")
    max_id = mysql_cur.fetchone()[0]

    wait_start = time.time()
    while True:
        pg_cur.execute("SELECT MAX(id) FROM pipeline.orders")
        pg_max = pg_cur.fetchone()[0] or 0
        if pg_max >= max_id:
            break
        elapsed = time.time() - wait_start
        print(f"  Waiting... PG max_id={pg_max}, MySQL max_id={max_id} ({elapsed:.1f}s)")
        time.sleep(1)

    total_latency = time.time() - start_time
    sync_latency = time.time() - (start_time + insert_time)

    print(f"\nResults:")
    print(f"  Total time: {total_latency:.2f}s")
    print(f"  Pipeline sync latency: {sync_latency:.2f}s")

    mysql_conn.close()
    pg_conn.close()

if __name__ == "__main__":
    run_load_test()
```

Run with Python 3.13 (venv activated):

**Linux (AlmaLinux 9) / macOS (M1/M2/M3):**

```bash
python scripts/load_test.py
```

**Windows PowerShell:**

```powershell
python scripts\load_test.py
```

---

## Step 10: Monitor the Pipeline

**Why:** After load testing, monitor consumer lag to ensure the pipeline is keeping up and no backlog is building.

Watch consumer lag:

**Linux (AlmaLinux 9) / macOS (M1/M2/M3):**

```bash
# Watch lag in real-time (updates every 2 seconds)
watch -n 2 'docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe \
  --group connect-postgres-sink'
```

**Windows PowerShell:**

```powershell
# Poll manually (no watch command on Windows natively)
while ($true) {
    docker exec kafka kafka-consumer-groups `
      --bootstrap-server localhost:9092 `
      --describe `
      --group connect-postgres-sink
    Start-Sleep 2
}
```

Check connector metrics:

**Linux (AlmaLinux 9) / macOS (M1/M2/M3):**

```bash
curl -s http://localhost:8083/connectors/postgres-sink/status | python3 -m json.tool
curl -s http://localhost:8083/connectors/mysql-cdc-source/status | python3 -m json.tool
```

**Windows PowerShell:**

```powershell
curl.exe -s http://localhost:8083/connectors/postgres-sink/status
curl.exe -s http://localhost:8083/connectors/mysql-cdc-source/status
```

Open Grafana dashboard: http://localhost:3000 (admin/admin)
- Import dashboard ID `7589` for Kafka consumer lag metrics

---

## Step 11: Simulate Failure and Recovery

### Test 1: MySQL Connection Loss

```bash
# Stop MySQL
docker compose stop mysql

# Watch connector status (will show FAILED after timeout)
sleep 30

curl -s http://localhost:8083/connectors/mysql-cdc-source/status | python3 -m json.tool
# or on Windows: curl.exe http://localhost:8083/connectors/mysql-cdc-source/status

# Restart MySQL
docker compose start mysql

# Debezium auto-reconnects within ~30s
sleep 30
curl -s http://localhost:8083/connectors/mysql-cdc-source/status | python3 -m json.tool

# Verify no data was lost
docker exec mysql-source mysql -u kafka_user -pkafka_password sourcedb -e "
INSERT INTO orders (customer_id, product_id, quantity, amount, status)
VALUES (1, 999, 1, 299.99, 'PENDING');
"
sleep 5
docker exec postgres-target psql -U kafka_user -d targetdb -c "
SELECT * FROM pipeline.orders WHERE amount = 299.99;
"
```

### Test 2: PostgreSQL Sink Failure

```bash
# Stop PostgreSQL
docker compose stop postgres

# Insert data into MySQL while PG is down
docker exec mysql-source mysql -u kafka_user -pkafka_password sourcedb -e "
INSERT INTO orders (customer_id, product_id, quantity, amount, status)
VALUES (2, 888, 5, 499.99, 'PENDING');
"

# Data is safe in Kafka (durable log)
echo "Data is in Kafka, waiting for PG to come back..."

# Restart PostgreSQL
docker compose start postgres
sleep 30

# Sink connector auto-recovers and catches up
curl -s http://localhost:8083/connectors/postgres-sink/status
# or on Windows: curl.exe http://localhost:8083/connectors/postgres-sink/status

# Verify data appeared
docker exec postgres-target psql -U kafka_user -d targetdb -c "
SELECT * FROM pipeline.orders WHERE amount = 499.99;
"
```

---

## Complete Pipeline Summary

```
MySQL sourcedb
├── orders (id, customer_id, product_id, amount, status, ...)
└── customers (id, name, email, phone, ...)
         |
         | binlog (ROW format, read by Debezium)
         v
Debezium MySQL Source Connector (in Kafka Connect)
├── Snapshot: reads all existing rows on first start
├── Stream: tails binlog for all subsequent changes
├── Transforms: ExtractNewRecordState (unwrap envelope) + RegexRouter
└── Error handling: DLQ for bad records
         |
         | JSON messages with schema (schemas.enable: true)
         v
Kafka Cluster
├── Topic: prod.mysql.sourcedb.orders   (3 partitions, RF=1)
├── Topic: prod.mysql.sourcedb.customers (3 partitions, RF=1)
└── Topic: prod.dlq.errors (1 partition, retain forever)
         |
         | JSON messages with schema
         v
Debezium JDBC Sink Connector (in Kafka Connect)
├── Mode: UPSERT on primary key (id)
├── RegexRouter: prod.mysql.sourcedb.orders -> orders
├── table.name.format: pipeline.${topic} -> pipeline.orders
├── schema.evolution: basic (auto-creates tables)
└── Error handling: retry 10x, then DLQ
         |
         | JDBC UPSERT
         v
PostgreSQL targetdb
├── pipeline.orders   (mirror of sourcedb.orders)
└── pipeline.customers (mirror of sourcedb.customers)
```

Performance (single-node dev):
- Snapshot throughput: ~5,000–20,000 rows/sec
- Streaming latency: less than 500ms (typically 100–200ms)
- Throughput (steady): ~1,000–5,000 events/sec per topic partition

---

## Checklist: Production Readiness

```
Infrastructure
  [ ] Multi-broker Kafka cluster (3+ brokers)
  [ ] Replication factor = 3, min.insync.replicas = 2
  [ ] KRaft controllers (3) separate from brokers
  [ ] Persistent storage (not ephemeral)
  [ ] Resource limits configured

Connectivity
  [ ] TLS encryption enabled
  [ ] SASL authentication configured
  [ ] ACLs defined per service account
  [ ] Secrets in Vault or K8s Secrets (not plaintext)

Data Integrity
  [ ] Schema Registry with BACKWARD compatibility
  [ ] Debezium heartbeat table configured
  [ ] DLQ topic configured for all connectors
  [ ] Sink uses UPSERT (idempotent)
  [ ] schemas.enable: true on both source and sink

Observability
  [ ] Prometheus metrics collected
  [ ] Grafana dashboards imported
  [ ] Consumer lag alerts configured
  [ ] Connector failure alerts configured

Operations
  [ ] Topic naming convention documented
  [ ] Connector deployment via CI/CD
  [ ] Runbook for common failures documented
  [ ] Data recovery procedure documented
```
