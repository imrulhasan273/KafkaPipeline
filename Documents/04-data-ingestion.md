# Data Ingestion

## Why This Document Exists

Data ingestion is the process of getting data from source systems into Kafka topics. This document covers all supported ingestion methods: CDC from MySQL (primary), CDC from PostgreSQL, CDC from Oracle, JDBC batch polling, and CSV file ingestion.

**Critical notes that apply to ALL source connectors:**

- `key.converter.schemas.enable` and `value.converter.schemas.enable` must be `"true"` — the Debezium JDBC Sink connector needs schema information embedded in each message. Setting these to `"false"` causes the sink to write 0 rows silently (no error, just nothing happens).
- `transforms.unwrap.drop.tombstones` must be `"true"` — when MySQL deletes a row, Debezium sends 2 messages: a rewrite record and a tombstone (null value). The Debezium JDBC Sink crashes on tombstones with `primary key mode 'record_value' cannot have null schema`. Setting `drop.tombstones: true` prevents tombstones from reaching the sink.

---

## 1. CDC from MySQL using Debezium

### Why CDC from MySQL?

**What CDC means:** Instead of running periodic queries (`SELECT * WHERE updated_at > last_run`), Debezium reads MySQL's binary log (binlog) in real-time. Every INSERT, UPDATE, and DELETE in MySQL is captured as an event and published to Kafka within milliseconds. This is how banking systems, e-commerce platforms, and analytics pipelines work at scale.

**What happens:** Debezium registers as a MySQL replica, reads the binlog, and publishes structured JSON events to Kafka topics. On first start it takes a full snapshot of existing rows, then streams new changes indefinitely.

### Prerequisites on MySQL (Option B — VPS)

> Skip this section if using Option A (Docker MySQL) — binlog is pre-configured via the `--log-bin` command in docker-compose.

```sql
-- Check binlog is enabled
SHOW VARIABLES LIKE 'log_bin';          -- should be ON
SHOW VARIABLES LIKE 'binlog_format';    -- should be ROW
SHOW VARIABLES LIKE 'binlog_row_image'; -- should be FULL

-- Verify kafka_user has CDC permissions
SHOW GRANTS FOR 'kafka_user'@'%';
-- Should include: REPLICATION SLAVE, REPLICATION CLIENT
```

### `connectors/source/mysql-cdc-source.json`

#### Option A — Docker MySQL

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

#### Option B — VPS MySQL

Change only `database.hostname` and `database.password`:

```json
{
  "name": "mysql-cdc-source",
  "config": {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "tasks.max": "1",

    "database.hostname": "62.171.177.208",
    "database.port": "3306",
    "database.user": "kafka_user",
    "database.password": "YOUR_VPS_PASSWORD",
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
| `database.hostname` | `"mysql"` | `"YOUR_VPS_IP"` (e.g. `"62.171.177.208"`) |
| `database.password` | `"kafka_password"` | `"YOUR_VPS_PASSWORD"` |

### Key config settings explained

| Setting | Value | Why |
|---------|-------|-----|
| `key.converter.schemas.enable` | `"true"` | **CRITICAL** — Sink needs schema info embedded in messages. `false` = sink writes 0 rows silently. |
| `value.converter.schemas.enable` | `"true"` | Same as above — both key and value schemas must be included. |
| `transforms.unwrap.drop.tombstones` | `"true"` | **CRITICAL** — Prevents null schema crash on deletes. |
| `transforms.unwrap.delete.handling.mode` | `"rewrite"` | Converts DELETE events to UPDATE with `__deleted: true` (soft delete / audit trail). |
| `snapshot.mode` | `"initial"` | Full snapshot on first start, then streams. Skip on subsequent restarts if offsets exist. |
| `database.server.id` | `"184054"` | Unique ID Debezium uses to register as a MySQL replica. Any unused number works. |

### Deploy the connector

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

Alternative (native PowerShell, no curl.exe needed):

```powershell
$body = Get-Content connectors/source/mysql-cdc-source.json -Raw
Invoke-RestMethod -Method Post `
  -Uri "http://localhost:8083/connectors" `
  -ContentType "application/json" `
  -Body $body
```

### Check connector status

**Linux (AlmaLinux 9) / macOS (M1/M2/M3):**

```bash
curl http://localhost:8083/connectors/mysql-cdc-source/status | python3 -m json.tool
```

**Windows PowerShell:**

```powershell
curl.exe http://localhost:8083/connectors/mysql-cdc-source/status
```

Expected output:

```json
{
  "name": "mysql-cdc-source",
  "connector": { "state": "RUNNING", "worker_id": "kafka-connect:8083" },
  "tasks": [{ "id": 0, "state": "RUNNING", "worker_id": "kafka-connect:8083" }],
  "type": "source"
}
```

Both `connector.state` and `tasks[0].state` must be `RUNNING`.

---

## 2. CDC from PostgreSQL using Debezium

### Why CDC from PostgreSQL?

PostgreSQL uses WAL (Write-Ahead Log) for CDC. Debezium uses the `pgoutput` plugin (built into PostgreSQL 10+) to stream WAL changes. No additional plugins need to be installed.

### Prerequisites on PostgreSQL

```sql
-- Check WAL level (must be logical)
SHOW wal_level;   -- should be 'logical'

-- Create replication user
CREATE USER kafka_user WITH REPLICATION LOGIN PASSWORD 'kafka_password';
GRANT SELECT ON ALL TABLES IN SCHEMA public TO kafka_user;

-- Create publication (Debezium uses pgoutput plugin)
CREATE PUBLICATION dbz_publication FOR TABLE public.orders, public.customers;

-- Verify
SELECT * FROM pg_publication;
```

Heartbeat table (prevents WAL slot from falling behind):

```sql
-- Run on source PostgreSQL
CREATE TABLE IF NOT EXISTS public.debezium_heartbeat (
    id INT PRIMARY KEY DEFAULT 1,
    ts TIMESTAMPTZ
);
INSERT INTO public.debezium_heartbeat VALUES (1, now());
```

### `connectors/source/postgres-cdc-source.json`

```json
{
  "name": "postgres-cdc-source",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "tasks.max": "1",

    "database.hostname": "postgres-source",
    "database.port": "5432",
    "database.user": "kafka_user",
    "database.password": "kafka_password",
    "database.dbname": "sourcedb",

    "topic.prefix": "prod.postgres",

    "plugin.name": "pgoutput",
    "publication.name": "dbz_publication",
    "slot.name": "debezium_slot",

    "table.include.list": "public.orders,public.customers",

    "snapshot.mode": "initial",
    "snapshot.isolation.mode": "read_committed",

    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "true",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "true",

    "transforms": "unwrap",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.add.fields": "op,ts_ms",
    "transforms.unwrap.delete.handling.mode": "rewrite",
    "transforms.unwrap.drop.tombstones": "true",

    "errors.tolerance": "all",
    "errors.deadletterqueue.topic.name": "prod.dlq.errors",

    "heartbeat.interval.ms": "10000",
    "heartbeat.action.query": "INSERT INTO public.debezium_heartbeat (ts) VALUES (now()) ON CONFLICT (id) DO UPDATE SET ts = now()"
  }
}
```

### Deploy the connector

**Linux (AlmaLinux 9) / macOS (M1/M2/M3):**

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @connectors/source/postgres-cdc-source.json
```

**Windows PowerShell:**

```powershell
curl.exe -X POST http://localhost:8083/connectors `
  -H "Content-Type: application/json" `
  -d "@connectors/source/postgres-cdc-source.json"
```

---

## 3. CDC from Oracle using Debezium (LogMiner)

### Why Oracle CDC?

Oracle uses LogMiner to read archived redo logs. This approach captures all DML changes (INSERT/UPDATE/DELETE) without requiring application changes. It requires Oracle 11g+ and specific user grants.

### Prerequisites on Oracle

```sql
-- Enable supplemental logging
ALTER SYSTEM SET enable_goldengate_replication=TRUE;
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

-- Create Debezium user (CDB-level for Oracle 12c+)
CREATE USER c##debezium IDENTIFIED BY dbz_password;
GRANT CREATE SESSION, SET CONTAINER TO c##debezium CONTAINER=ALL;
GRANT SELECT ON V_$DATABASE TO c##debezium CONTAINER=ALL;
GRANT FLASHBACK ANY TABLE TO c##debezium CONTAINER=ALL;
GRANT SELECT ANY TABLE TO c##debezium CONTAINER=ALL;
GRANT SELECT_CATALOG_ROLE TO c##debezium CONTAINER=ALL;
GRANT EXECUTE_CATALOG_ROLE TO c##debezium CONTAINER=ALL;
GRANT SELECT ANY TRANSACTION TO c##debezium CONTAINER=ALL;
GRANT LOGMINING TO c##debezium CONTAINER=ALL;
```

### `connectors/source/oracle-cdc-source.json`

```json
{
  "name": "oracle-cdc-source",
  "config": {
    "connector.class": "io.debezium.connector.oracle.OracleConnector",
    "tasks.max": "1",

    "database.hostname": "oracle-host",
    "database.port": "1521",
    "database.user": "c##debezium",
    "database.password": "dbz_password",
    "database.dbname": "ORCLCDB",
    "database.pdb.name": "ORCLPDB1",

    "topic.prefix": "prod.oracle",

    "table.include.list": "FINANCE.INVOICES,FINANCE.PAYMENTS",

    "log.mining.strategy": "online_catalog",
    "log.mining.continuous.mine": "true",

    "snapshot.mode": "initial",

    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "true",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "true",

    "transforms": "unwrap",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.add.fields": "op,ts_ms",
    "transforms.unwrap.drop.tombstones": "true"
  }
}
```

### Deploy the connector

**Linux (AlmaLinux 9) / macOS (M1/M2/M3):**

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @connectors/source/oracle-cdc-source.json
```

**Windows PowerShell:**

```powershell
curl.exe -X POST http://localhost:8083/connectors `
  -H "Content-Type: application/json" `
  -d "@connectors/source/oracle-cdc-source.json"
```

---

## 4. JDBC Source Connector (Batch / Polling)

### Why JDBC batch?

For databases without CDC capability (no binlog, no WAL access), or when you only need scheduled batch loads and sub-second latency is not required. JDBC polling runs periodic `SELECT` queries using an incrementing column and/or timestamp column to detect new/changed rows.

**Limitation:** JDBC batch cannot detect DELETEs — only INSERTs and UPDATEs (via timestamp column). Use CDC for full change capture.

### `connectors/source/jdbc-batch-source.json`

```json
{
  "name": "jdbc-batch-source",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
    "tasks.max": "1",

    "connection.url": "jdbc:postgresql://postgres-source:5432/sourcedb",
    "connection.user": "kafka_user",
    "connection.password": "kafka_password",

    "mode": "timestamp+incrementing",
    "timestamp.column.name": "updated_at",
    "incrementing.column.name": "id",
    "table.whitelist": "public.orders,public.customers",

    "topic.prefix": "prod.batch.",
    "poll.interval.ms": "60000",
    "batch.max.rows": "5000",
    "numeric.mapping": "best_fit",
    "timestamp.delay.interval.ms": "1000",

    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "true"
  }
}
```

---

## 5. File Ingestion (CSV to Kafka)

### Option A: SpoolDir Connector (recommended for structured files)

**Why:** The SpoolDir connector watches a directory for new CSV files and produces each row as a Kafka message. Files are moved to a "processed" directory after ingestion.

```json
{
  "name": "spooldir-csv-source",
  "config": {
    "connector.class": "com.github.jcustenborder.kafka.connect.spooldir.SpoolDirCsvSourceConnector",
    "tasks.max": "1",

    "input.path": "/data/input",
    "finished.path": "/data/processed",
    "error.path": "/data/error",
    "input.file.pattern": ".*\\.csv",

    "topic": "prod.files.csv.raw",

    "csv.first.row.as.header": "true",
    "csv.null.field.indicator": "EMPTY_SEPARATORS",

    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "true",

    "schema.generation.enabled": "true",
    "schema.generation.key.fields": "id",

    "errors.tolerance": "all",
    "errors.deadletterqueue.topic.name": "prod.dlq.errors"
  }
}
```

### Option B: Custom Python Producer for CSV

**Why:** Use when you need custom parsing logic, field transformations, or the SpoolDir connector is not available in your Kafka Connect image.

**What happens:** The script reads a CSV file row by row, serializes each row as Avro (with schema registered in Schema Registry), and produces it to Kafka. Progress is logged every 1000 rows.

```python
# scripts/csv_producer.py
import csv
import json
import uuid
from pathlib import Path
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

KAFKA_BOOTSTRAP = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
TOPIC = "prod.files.csv.raw"

AVRO_SCHEMA = """
{
  "type": "record",
  "name": "CsvRow",
  "namespace": "com.pipeline.files",
  "fields": [
    {"name": "id",         "type": ["null", "string"], "default": null},
    {"name": "name",       "type": ["null", "string"], "default": null},
    {"name": "amount",     "type": ["null", "string"], "default": null},
    {"name": "created_at", "type": ["null", "string"], "default": null},
    {"name": "_source_file", "type": "string"},
    {"name": "_row_number",  "type": "int"}
  ]
}
"""

def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")

def produce_csv(file_path: str):
    sr_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    serializer = AvroSerializer(sr_client, AVRO_SCHEMA)
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

    file_name = Path(file_path).name
    with open(file_path, newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row_num, row in enumerate(reader, start=1):
            record = dict(row)
            record["_source_file"] = file_name
            record["_row_number"] = row_num

            producer.produce(
                topic=TOPIC,
                key=str(uuid.uuid4()),
                value=serializer(record, SerializationContext(TOPIC, MessageField.VALUE)),
                on_delivery=delivery_report
            )

            if row_num % 1000 == 0:
                producer.poll(0)
                print(f"Produced {row_num} rows...")

    producer.flush()
    print(f"Finished producing {row_num} rows from {file_name}")

if __name__ == "__main__":
    import sys
    produce_csv(sys.argv[1])
```

Run (Python 3.13, venv activated):

**Linux (AlmaLinux 9) / macOS (M1/M2/M3):**

```bash
python scripts/csv_producer.py /data/input/orders_2024.csv
```

**Windows PowerShell:**

```powershell
python scripts\csv_producer.py C:\data\input\orders_2024.csv
```

---

## 6. Snapshot Mode Reference

| Mode | Description | Use When |
|------|-------------|----------|
| `initial` | Full snapshot on first start, then stream | Default for new connectors |
| `initial_only` | Snapshot only, no streaming | One-time full export |
| `never` | Skip snapshot, only stream new changes | Already migrated, just track changes |
| `when_needed` | Snapshot if no stored offset | Safe restart default |
| `schema_only` | Read schema only, no data snapshot | High data volume, stream from now |
| `recovery` | Re-snapshot specific tables | After data loss recovery |

---

## 7. Debezium Event Structure Reference

### Full CDC Event (before `unwrap` transform)

**What:** The raw Debezium message contains both the before and after state of the row, plus rich metadata about the source system.

```json
{
  "schema": { "..." },
  "payload": {
    "before": null,
    "after": {
      "id": 1,
      "customer_id": 42,
      "amount": "99.99",
      "status": "PENDING"
    },
    "source": {
      "version": "2.6.0",
      "connector": "mysql",
      "name": "prod.mysql",
      "ts_ms": 1700000000000,
      "db": "sourcedb",
      "table": "orders",
      "server_id": 184054,
      "file": "mysql-bin.000001",
      "pos": 12345
    },
    "op": "c",
    "ts_ms": 1700000001234,
    "transaction": null
  }
}
```

### After `ExtractNewRecordState` (unwrap) transform

**What:** The `unwrap` transform flattens the nested payload to a simple flat record. The `after` fields become top-level fields and CDC metadata is added as `__` prefixed fields.

```json
{
  "id": 1,
  "customer_id": 42,
  "amount": "99.99",
  "status": "PENDING",
  "__op": "c",
  "__ts_ms": 1700000000000,
  "__source_db": "sourcedb",
  "__source_table": "orders"
}
```

**Operation codes:**
- `c` = create (INSERT)
- `u` = update (UPDATE)
- `d` = delete (DELETE)
- `r` = read (snapshot row)

---

## 8. Connector Management REST API

All connector management uses the Kafka Connect REST API at `http://localhost:8083`.

### List, status, delete

**Linux (AlmaLinux 9) / macOS (M1/M2/M3):**

```bash
# List all connectors
curl http://localhost:8083/connectors

# Check status
curl http://localhost:8083/connectors/mysql-cdc-source/status | python3 -m json.tool

# Delete a connector
curl -X DELETE http://localhost:8083/connectors/mysql-cdc-source

# Restart a connector
curl -X POST http://localhost:8083/connectors/mysql-cdc-source/restart

# Pause a connector
curl -X PUT http://localhost:8083/connectors/mysql-cdc-source/pause

# Resume a connector
curl -X PUT http://localhost:8083/connectors/mysql-cdc-source/resume
```

**Windows PowerShell:**

```powershell
# List all connectors
curl.exe http://localhost:8083/connectors

# Check status
curl.exe http://localhost:8083/connectors/mysql-cdc-source/status

# Delete a connector
curl.exe -X DELETE http://localhost:8083/connectors/mysql-cdc-source

# Restart a connector
curl.exe -X POST http://localhost:8083/connectors/mysql-cdc-source/restart

# Pause a connector
curl.exe -X PUT http://localhost:8083/connectors/mysql-cdc-source/pause

# Resume a connector
curl.exe -X PUT http://localhost:8083/connectors/mysql-cdc-source/resume
```

---

## Common Errors

**Error: Connector state is FAILED — `Access denied for user 'kafka_user'@'%' to database 'sourcedb'`**
- The user doesn't have the required privileges.
- Fix: Grant `REPLICATION SLAVE, REPLICATION CLIENT` (and `SELECT` on the database) in MySQL.

**Error: Messages in Kafka have no schema (just raw values, not `{"schema":..., "payload":...}`)**
- Cause: `schemas.enable` is `false` somewhere.
- Fix: Ensure both `key.converter.schemas.enable: "true"` and `value.converter.schemas.enable: "true"` are set. After fixing, delete the connector + topics + recreate (old schemaless messages must be cleared).

**Error: Sink connector task FAILED with `cannot have null schema`**
- Cause: A tombstone message (null value, null key schema) reached the sink.
- Fix: Set `transforms.unwrap.drop.tombstones: "true"` in the source connector config.

**Error: `Connector mysql-cdc-source already exists` (error_code: 409)**
- The connector is already registered. This is normal — connector configs are stored in Kafka's `_connect-configs` topic and survive restarts.
- If you want to update the config, delete it first: `curl -X DELETE http://localhost:8083/connectors/mysql-cdc-source`

**Snapshot skips existing data on re-registration**
- `snapshot.mode: initial` only snapshots if NO stored offsets exist for this connector name.
- Fix: Delete and re-register with a new connector name, or trigger real MySQL UPDATEs to force CDC events.
