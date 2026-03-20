# Data Sink / Delivery

## Why This Document Exists

The sink connector reads messages from Kafka topics and writes them to the target database. This document covers the Debezium JDBC Sink Connector (the correct connector bundled in `debezium/connect:2.6`), a custom Python consumer for complex use cases, and the full connector management REST API.

---

## Debezium JDBC Sink Connector — Why Not Confluent?

**Why:** The `debezium/connect:2.6` Docker image bundles `io.debezium.connector.jdbc.JdbcSinkConnector`. The Confluent `io.confluent.connect.jdbc.JdbcSinkConnector` is NOT included in this image. Attempting to use the Confluent class name causes a `connector.class not found` error at startup.

The Debezium JDBC Sink also uses different property names:

| Property | Confluent JDBC (WRONG for this image) | Debezium JDBC (CORRECT) |
|----------|--------------------------------------|------------------------|
| Connector class | `io.confluent.connect.jdbc.JdbcSinkConnector` | `io.debezium.connector.jdbc.JdbcSinkConnector` |
| DB username | `connection.user` | `connection.username` |
| Primary key mode | `pk.mode` | `primary.key.mode` |
| PK fields | `pk.fields` | `primary.key.fields` |
| Auto create tables | `auto.create: true` | `schema.evolution: basic` |

**What the RegexRouter transform does:** The Kafka topic name is `prod.mysql.sourcedb.orders`. Without transformation, the sink would try to create a PostgreSQL table named `prod.mysql.sourcedb.orders` (containing dots — invalid in PostgreSQL). The RegexRouter strips the prefix and maps `prod.mysql.sourcedb.orders` → `orders`. Then `table.name.format: pipeline.${topic}` creates the full table name `pipeline.orders`.

---

## 1. Sink to PostgreSQL

### `connectors/sink/postgres-sink.json`

#### Option A — Docker PostgreSQL

**What:** Uses the PostgreSQL container (`postgres-target`, reachable inside Docker network as `postgres`).

**What happens:** The sink reads from `prod.mysql.sourcedb.orders` and `prod.mysql.sourcedb.customers`, routes topic names through RegexRouter to get `orders`/`customers`, creates `pipeline.orders` and `pipeline.customers` automatically (via `schema.evolution: basic`), and UPSERTs rows based on the `id` primary key.

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

#### Option B — VPS PostgreSQL

Change only `connection.url` and `connection.password`:

```json
{
  "name": "postgres-sink",
  "config": {
    "connector.class": "io.debezium.connector.jdbc.JdbcSinkConnector",
    "tasks.max": "2",

    "connection.url": "jdbc:postgresql://62.171.177.208:5432/targetdb",
    "connection.username": "kafka_user",
    "connection.password": "YOUR_VPS_PASSWORD",

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

| Field | Option A (Docker) | Option B (VPS) |
|-------|------------------|----------------|
| `connection.url` | `"jdbc:postgresql://postgres:5432/targetdb"` | `"jdbc:postgresql://YOUR_VPS_IP:5432/targetdb"` |
| `connection.password` | `"kafka_password"` | `"YOUR_VPS_PASSWORD"` |

Test VPS connectivity before registering:

```powershell
# Windows PowerShell
Test-NetConnection -ComputerName 62.171.177.208 -Port 5432
# TcpTestSucceeded: True = reachable
```

### Deploy the sink connector

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

Alternative (native PowerShell):

```powershell
$body = Get-Content connectors/sink/postgres-sink.json -Raw
Invoke-RestMethod -Method Post `
  -Uri "http://localhost:8083/connectors" `
  -ContentType "application/json" `
  -Body $body
```

### Verify sink connector is running

**Linux (AlmaLinux 9) / macOS (M1/M2/M3):**

```bash
curl http://localhost:8083/connectors/postgres-sink/status | python3 -m json.tool
```

**Windows PowerShell:**

```powershell
curl.exe http://localhost:8083/connectors/postgres-sink/status
```

Expected: both `connector.state` and all `tasks[*].state` are `RUNNING`.

### Verify data synced to PostgreSQL

```bash
docker exec postgres-target psql -U kafka_user -d targetdb -c "SELECT * FROM pipeline.orders ORDER BY id;"
docker exec postgres-target psql -U kafka_user -d targetdb -c "SELECT COUNT(*) FROM pipeline.customers;"
```

---

## 2. Sink to MySQL

**Why:** Used when the target system is MySQL (e.g., replicating from PostgreSQL source to MySQL target).

```json
{
  "name": "mysql-sink",
  "config": {
    "connector.class": "io.debezium.connector.jdbc.JdbcSinkConnector",
    "tasks.max": "2",

    "connection.url": "jdbc:mysql://mysql-target:3306/targetdb?useSSL=false&allowPublicKeyRetrieval=true&rewriteBatchedStatements=true",
    "connection.username": "kafka_user",
    "connection.password": "kafka_password",

    "topics": "prod.postgres.sourcedb.orders",

    "insert.mode": "upsert",
    "primary.key.mode": "record_value",
    "primary.key.fields": "id",

    "schema.evolution": "basic",
    "table.name.format": "${topic}",

    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "true",

    "batch.size": "5000",
    "max.retries": "5",
    "retry.backoff.ms": "2000",

    "errors.tolerance": "all",
    "errors.deadletterqueue.topic.name": "prod.dlq.errors"
  }
}
```

---

## 3. Sink to SQL Server

```json
{
  "name": "sqlserver-sink",
  "config": {
    "connector.class": "io.debezium.connector.jdbc.JdbcSinkConnector",
    "tasks.max": "2",

    "connection.url": "jdbc:sqlserver://sqlserver:1433;databaseName=targetdb;encrypt=false",
    "connection.username": "sa",
    "connection.password": "SqlServerPassword123!",

    "topics": "prod.oracle.finance.invoices",

    "insert.mode": "upsert",
    "primary.key.mode": "record_value",
    "primary.key.fields": "INVOICE_ID",

    "schema.evolution": "basic",
    "table.name.format": "dbo.${topic}",

    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "true",

    "batch.size": "2000",

    "errors.tolerance": "all",
    "errors.deadletterqueue.topic.name": "prod.dlq.errors"
  }
}
```

---

## 4. Custom Consumer (Python) — Full Control

**Why:** Use a custom Python consumer when you need complex transformations, multi-step lookups, CDC metadata columns (like `_cdc_op`, `_ingested_at`), or non-JDBC sinks (Elasticsearch, Redis, S3, etc.).

**What happens:** The consumer reads batches of 500 messages, separates UPSERTs from DELETEs, and applies them to PostgreSQL in a single transaction. It commits Kafka offsets only after the database transaction succeeds.

```python
# consumers/postgres_consumer.py
import psycopg2
import psycopg2.extras
import logging
import signal
import sys
from confluent_kafka import Consumer, KafkaError
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_CONFIG = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "postgres-consumer-group",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
    "max.poll.interval.ms": 300000,
    "session.timeout.ms": 30000,
}

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
INSERT INTO pipeline.orders (id, customer_id, product_id, quantity, amount, status, created_at, updated_at, _cdc_op, _ingested_at)
VALUES (%(id)s, %(customer_id)s, %(product_id)s, %(quantity)s, %(amount)s, %(status)s, %(created_at)s, %(updated_at)s, %(__op)s, NOW())
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
    logger.info("Shutdown signal received")
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

            # Value is JSON with schema+payload — extract the payload
            raw = json.loads(msg.value())
            value = raw.get("payload", raw)  # handle both wrapped and unwrapped

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

Run (Python 3.13, venv activated):

**Linux (AlmaLinux 9) / macOS (M1/M2/M3):**

```bash
python consumers/postgres_consumer.py
```

**Windows PowerShell:**

```powershell
python consumers\postgres_consumer.py
```

---

## 5. Topic-to-Table Mapping Strategies

### Strategy 1: One Topic → One Table (default with RegexRouter)

```
prod.mysql.sourcedb.orders    → pipeline.orders     (via RegexRouter + table.name.format)
prod.mysql.sourcedb.customers → pipeline.customers
```

### Strategy 2: Multiple Topics → One Table (merge)

```json
{
  "topics.regex": "prod\\.(mysql|postgres)\\..*\\.orders",
  "table.name.format": "pipeline.orders_merged"
}
```

### Strategy 3: One Topic → Multiple Tables

Use separate connector instances with topic filtering, or a custom Python consumer that inspects the record and routes accordingly.

---

## 6. Connector Management REST API

All commands work the same inside containers on any OS. The `curl` vs `curl.exe` distinction only applies on the host machine.

### List, status, restart

**Linux (AlmaLinux 9) / macOS (M1/M2/M3):**

```bash
# List all connectors
curl http://localhost:8083/connectors

# Get connector config
curl http://localhost:8083/connectors/postgres-sink/config | python3 -m json.tool

# Get connector status
curl http://localhost:8083/connectors/postgres-sink/status | python3 -m json.tool

# Pause a connector
curl -X PUT http://localhost:8083/connectors/postgres-sink/pause

# Resume a connector
curl -X PUT http://localhost:8083/connectors/postgres-sink/resume

# Restart a connector
curl -X POST http://localhost:8083/connectors/postgres-sink/restart

# Restart a specific task
curl -X POST http://localhost:8083/connectors/postgres-sink/tasks/0/restart

# Delete a connector
curl -X DELETE http://localhost:8083/connectors/postgres-sink

# Update connector config
curl -X PUT http://localhost:8083/connectors/postgres-sink/config \
  -H "Content-Type: application/json" \
  -d '{"batch.size": "5000", "connector.class": "io.debezium.connector.jdbc.JdbcSinkConnector"}'
```

**Windows PowerShell:**

```powershell
# List all connectors
curl.exe http://localhost:8083/connectors

# Get connector config
curl.exe http://localhost:8083/connectors/postgres-sink/config

# Get connector status
curl.exe http://localhost:8083/connectors/postgres-sink/status

# Pause a connector
curl.exe -X PUT http://localhost:8083/connectors/postgres-sink/pause

# Resume a connector
curl.exe -X PUT http://localhost:8083/connectors/postgres-sink/resume

# Restart a connector
curl.exe -X POST http://localhost:8083/connectors/postgres-sink/restart

# Restart a specific task
curl.exe -X POST http://localhost:8083/connectors/postgres-sink/tasks/0/restart

# Delete a connector
curl.exe -X DELETE http://localhost:8083/connectors/postgres-sink
```

---

## Common Errors

**Error: `connector.class not found: io.confluent.connect.jdbc.JdbcSinkConnector`**
- The Confluent JDBC sink is not included in `debezium/connect:2.6`.
- Fix: Use `io.debezium.connector.jdbc.JdbcSinkConnector` as the connector class.

**Error: `Unknown field: pk.mode`**
- Using Confluent JDBC property names with the Debezium connector.
- Fix: Use `primary.key.mode` (not `pk.mode`) and `primary.key.fields` (not `pk.fields`).

**Error: Sink writes 0 rows, no errors in logs**
- Cause: `schemas.enable` is `false` in the source connector. The sink receives schemaless messages and cannot determine table structure.
- Fix: Set `key.converter.schemas.enable: "true"` and `value.converter.schemas.enable: "true"` in the source connector. Then delete the connector + topics + recreate.

**Error: Task FAILED with `primary key mode 'record_value' cannot have null schema`**
- Cause: A tombstone message (null key schema) from a MySQL DELETE reached the sink.
- Fix: Set `transforms.unwrap.drop.tombstones: "true"` in the source connector.

**Error: `must be owner of table orders` (PostgreSQL)**
- A table in `pipeline` schema was created by the `postgres` superuser, not `kafka_user`.
- Fix: Drop the table and let the sink recreate it: `DROP TABLE IF EXISTS pipeline.orders;` — then restart the sink connector tasks.

**Error: Tasks FAILED with `duplicate key value violates unique constraint pg_type_typname_nsp_index`**
- The connector lost its state and tried to recreate an existing table.
- Fix: Drop `pipeline.orders` in PostgreSQL, then restart tasks.
