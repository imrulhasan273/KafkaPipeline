# Data Ingestion

## 1. CDC from MySQL using Debezium

### Prerequisites on MySQL

```sql
-- Check binlog is enabled
SHOW VARIABLES LIKE 'log_bin';          -- should be ON
SHOW VARIABLES LIKE 'binlog_format';    -- should be ROW
SHOW VARIABLES LIKE 'binlog_row_image'; -- should be FULL

-- Create dedicated replication user
CREATE USER 'debezium'@'%' IDENTIFIED WITH mysql_native_password BY 'dbz_password';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'debezium'@'%';
FLUSH PRIVILEGES;
```

### `connectors/source/mysql-cdc-source.json`

```json
{
  "name": "mysql-cdc-source",
  "config": {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "tasks.max": "1",

    "database.hostname": "mysql",
    "database.port": "3306",
    "database.user": "debezium",
    "database.password": "dbz_password",
    "database.server.id": "184054",
    "database.server.name": "prod.mysql",

    "topic.prefix": "prod.mysql",
    "database.include.list": "sourcedb",
    "table.include.list": "sourcedb.orders,sourcedb.customers,sourcedb.products",

    "schema.history.internal.kafka.bootstrap.servers": "kafka:9092",
    "schema.history.internal.kafka.topic": "_schema-changes.mysql",

    "include.schema.changes": "true",
    "snapshot.mode": "initial",
    "snapshot.locking.mode": "minimal",

    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable: "false"
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable: "false"

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

### Deploy connector

```bash
# Register the connector
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @connectors/source/mysql-cdc-source.json

# Check status
curl http://localhost:8083/connectors/mysql-cdc-source/status | python3 -m json.tool

# Expected output:
# {
#   "name": "mysql-cdc-source",
#   "connector": { "state": "RUNNING", "worker_id": "..." },
#   "tasks": [{ "id": 0, "state": "RUNNING", "worker_id": "..." }]
# }
```

---

## 2. CDC from PostgreSQL using Debezium

### Prerequisites on PostgreSQL

```sql
-- Check WAL level (must be logical)
SHOW wal_level;   -- should be 'logical'

-- Create replication user
CREATE USER debezium WITH REPLICATION LOGIN PASSWORD 'dbz_password';
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium;

-- Create publication (Debezium uses pgoutput plugin)
CREATE PUBLICATION dbz_publication FOR TABLE public.orders, public.customers;

-- Verify
SELECT * FROM pg_publication;
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
    "database.user": "debezium",
    "database.password": "dbz_password",
    "database.dbname": "sourcedb",

    "topic.prefix": "prod.postgres",

    "plugin.name": "pgoutput",
    "publication.name": "dbz_publication",
    "slot.name": "debezium_slot",

    "table.include.list": "public.orders,public.customers",

    "snapshot.mode": "initial",
    "snapshot.isolation.mode": "read_committed",

    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable: "false"
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable: "false"

    "transforms": "unwrap",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.add.fields": "op,ts_ms",
    "transforms.unwrap.delete.handling.mode": "rewrite",

    "errors.tolerance": "all",
    "errors.deadletterqueue.topic.name": "prod.dlq.errors",

    "heartbeat.interval.ms": "10000",
    "heartbeat.action.query": "INSERT INTO public.debezium_heartbeat (ts) VALUES (now()) ON CONFLICT (id) DO UPDATE SET ts = now()"
  }
}
```

### Heartbeat table (prevents WAL slot from falling behind)

```sql
-- Run on source PostgreSQL
CREATE TABLE IF NOT EXISTS public.debezium_heartbeat (
    id INT PRIMARY KEY DEFAULT 1,
    ts TIMESTAMPTZ
);
INSERT INTO public.debezium_heartbeat VALUES (1, now());
```

---

## 3. CDC from Oracle using Debezium (LogMiner)

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
    "key.converter.schemas.enable: "false"
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable: "false"

    "transforms": "unwrap",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.add.fields": "op,ts_ms"
  }
}
```

---

## 4. JDBC Source Connector (Batch / Polling)

For databases without CDC capability or for scheduled batch loads.

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
    "value.converter.schemas.enable: "false"
  }
}
```

---

## 5. File Ingestion (CSV → Kafka)

### Option A: SpoolDir Connector (recommended for structured files)

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
    "value.converter.schemas.enable: "false"

    "schema.generation.enabled": "true",
    "schema.generation.key.fields": "id",

    "errors.tolerance": "all",
    "errors.deadletterqueue.topic.name": "prod.dlq.errors"
  }
}
```

### Option B: Custom Python Producer for CSV

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

```bash
# Usage
python scripts/csv_producer.py /data/input/orders_2024.csv
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
