# Data Sink / Delivery

## JDBC Sink Connector Overview

The Confluent JDBC Sink Connector writes Kafka records to any JDBC-compatible database. It supports:
- **Insert mode**: append-only
- **Upsert mode**: insert or update based on primary key
- **Delete mode**: tombstone records → DELETE in database

---

## 1. Sink to PostgreSQL

### `connectors/sink/postgres-sink.json`

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
    "topics.regex": "",

    "insert.mode": "upsert",
    "pk.mode": "record_value",
    "pk.fields": "id",

    "auto.create": "true",
    "auto.evolve": "true",

    "table.name.format": "pipeline.${topic}",

    "key.converter": "io.confluent.connect.avro.AvroConverter",
    "key.converter.schema.registry.url": "http://schema-registry:8081",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://schema-registry:8081",

    "transforms": "route",
    "transforms.route.type": "org.apache.kafka.connect.transforms.ReplaceField$Value",
    "transforms.route.exclude": "__op,__ts_ms,__source_db,__source_table,_pipeline_version",

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

### With Delete Support

```json
{
  "name": "postgres-sink-with-deletes",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",

    "connection.url": "jdbc:postgresql://postgres:5432/targetdb",
    "connection.user": "kafka_user",
    "connection.password": "kafka_password",

    "topics": "prod.mysql.sourcedb.orders",

    "insert.mode": "upsert",
    "pk.mode": "record_value",
    "pk.fields": "id",

    "delete.enabled": "true",

    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://schema-registry:8081",

    "transforms": "extractValue",
    "transforms.extractValue.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.extractValue.drop.tombstones": "false",
    "transforms.extractValue.delete.handling.mode": "tombstone"
  }
}
```

---

## 2. Sink to MySQL

```json
{
  "name": "mysql-sink",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "tasks.max": "2",

    "connection.url": "jdbc:mysql://mysql-target:3306/targetdb?useSSL=false&allowPublicKeyRetrieval=true&rewriteBatchedStatements=true",
    "connection.user": "kafka_user",
    "connection.password": "kafka_password",

    "topics": "prod.postgres.sourcedb.orders",

    "insert.mode": "upsert",
    "pk.mode": "record_value",
    "pk.fields": "id",

    "auto.create": "true",
    "auto.evolve": "true",

    "table.name.format": "${topic}",

    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://schema-registry:8081",

    "dialect.name": "MySqlDatabaseDialect",
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
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "tasks.max": "2",

    "connection.url": "jdbc:sqlserver://sqlserver:1433;databaseName=targetdb;encrypt=false",
    "connection.user": "sa",
    "connection.password": "SqlServerPassword123!",

    "topics": "prod.oracle.finance.invoices",

    "insert.mode": "upsert",
    "pk.mode": "record_value",
    "pk.fields": "INVOICE_ID",

    "auto.create": "true",
    "auto.evolve": "true",

    "table.name.format": "dbo.${topic}",

    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://schema-registry:8081",

    "dialect.name": "SqlServerDatabaseDialect",
    "batch.size": "2000",

    "errors.tolerance": "all",
    "errors.deadletterqueue.topic.name": "prod.dlq.errors"
  }
}
```

---

## 4. Custom Consumer (Python) — Full Control

For complex transformations, multi-step lookups, or non-JDBC sinks.

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

---

## 5. Topic-to-Table Mapping Strategies

### Strategy 1: One Topic → One Table (default)
```
prod.mysql.sourcedb.orders    → pipeline.orders
prod.mysql.sourcedb.customers → pipeline.customers
```

### Strategy 2: Multiple Topics → One Table (merge)
Use `topics.regex` with routing transforms:
```json
{
  "topics.regex": "prod\\.(mysql|postgres)\\..*\\.orders",
  "table.name.format": "pipeline.orders_merged"
}
```

### Strategy 3: One Topic → Multiple Tables
Use separate connector instances with topic filtering, or a custom consumer that inspects the record and routes accordingly.

---

## 6. Connector Management REST API

```bash
# List all connectors
curl http://localhost:8083/connectors

# Get connector config
curl http://localhost:8083/connectors/postgres-sink/config

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
  -d '{"batch.size": "5000", ...}'
```

---

## 7. JDBC Driver Installation

```bash
# Install JDBC drivers into Kafka Connect container
docker exec kafka-connect confluent-hub install confluentinc/kafka-connect-jdbc:10.7.4

# Or mount JARs via Docker volume
# Add to docker-compose:
#   volumes:
#     - ./jars/postgresql-42.7.1.jar:/usr/share/java/kafka-connect-jdbc/postgresql-42.7.1.jar
#     - ./jars/mysql-connector-j-8.3.0.jar:/usr/share/java/kafka-connect-jdbc/mysql-connector-j-8.3.0.jar
#     - ./jars/mssql-jdbc-12.4.0.jre11.jar:/usr/share/java/kafka-connect-jdbc/mssql-jdbc-12.4.0.jre11.jar

# Restart connector after installing
docker restart kafka-connect
```
