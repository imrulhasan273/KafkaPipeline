# Real-World Example: MySQL → Kafka → PostgreSQL (CDC Pipeline)

## Overview

End-to-end implementation of a production-grade CDC pipeline:
- **Source**: MySQL 8.0 (`sourcedb.orders`, `sourcedb.customers`)
- **Transport**: Apache Kafka with Schema Registry
- **Sink**: PostgreSQL 16 (`targetdb.pipeline.orders`, `targetdb.pipeline.customers`)
- **Scenario**: E-commerce order replication with real-time sync

---

## Step 1: Start the Environment

```bash
# Clone or create project directory
mkdir -p kafka-pipeline/{connectors/{source,sink},scripts,monitoring/grafana/dashboards,config}
cd kafka-pipeline

# Start all services (from 03-environment-setup.md docker-compose.yml)
docker-compose up -d

# Wait for services to be healthy
watch docker-compose ps

# Verify all services are RUNNING/healthy
# Expected: kafka, schema-registry, kafka-connect, kafka-ui, mysql, postgres
```

---

## Step 2: Verify Source Database

```bash
# Connect to MySQL
docker exec -it mysql-source mysql -u kafka_user -pkafka_password sourcedb

# Verify CDC user permissions
SHOW GRANTS FOR 'debezium'@'%';

# Verify binlog is enabled
SHOW VARIABLES LIKE 'log_bin';
SHOW VARIABLES LIKE 'binlog_format';
SHOW VARIABLES LIKE 'binlog_row_image';

# Check current data
SELECT * FROM orders LIMIT 5;
SELECT * FROM customers LIMIT 5;
EXIT;
```

Expected output:
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

```bash
# Run topic creation script
bash scripts/create-topics.sh

# Verify topics were created
docker exec kafka kafka-topics \
  --list \
  --bootstrap-server localhost:9092

# Expected topics:
# prod.mysql.sourcedb.orders
# prod.mysql.sourcedb.customers
# prod.dlq.errors
# _connect-configs, _connect-offsets, _connect-status
```

---

## Step 4: Deploy Debezium MySQL CDC Source Connector

```bash
# Deploy source connector
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @connectors/source/mysql-cdc-source.json

# Wait ~10 seconds, then check status
sleep 10
curl -s http://localhost:8083/connectors/mysql-cdc-source/status | python3 -m json.tool
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

---

## Step 5: Verify Data in Kafka

```bash
# The connector first takes a snapshot — watch the topic fill up
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic prod.mysql.sourcedb.orders \
  --from-beginning \
  --max-messages 5 \
  --property print.headers=true

# Check how many messages were produced
docker exec kafka kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 \
  --topic prod.mysql.sourcedb.orders
```

Open Kafka UI: http://localhost:8080
- Navigate to Topics → `prod.mysql.sourcedb.orders`
- You should see the initial snapshot messages

---

## Step 6: Deploy PostgreSQL Sink Connector

```bash
# Deploy sink connector
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @connectors/sink/postgres-sink.json

# Check status
sleep 10
curl -s http://localhost:8083/connectors/postgres-sink/status | python3 -m json.tool
```

---

## Step 7: Verify Data in PostgreSQL

```bash
# Connect to PostgreSQL target
docker exec -it postgres-target psql -U kafka_user -d targetdb

-- Check data was synced
SELECT COUNT(*) FROM pipeline.orders;
SELECT COUNT(*) FROM pipeline.customers;

SELECT id, customer_id, amount, status, _cdc_op, _ingested_at
FROM pipeline.orders
ORDER BY id;

EXIT
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
SELECT id, customer_id, amount, status, _cdc_op, _ingested_at
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
SELECT id, status, _cdc_op, _ingested_at
FROM pipeline.orders
ORDER BY id DESC
LIMIT 3;
"
```

Expected: `_cdc_op` = `u` and status changed to `PROCESSING`

### Delete an order

```bash
docker exec mysql-source mysql -u kafka_user -pkafka_password sourcedb -e "
DELETE FROM orders WHERE id = 1;
"

sleep 2

# Verify row was deleted in PostgreSQL
docker exec postgres-target psql -U kafka_user -d targetdb -c "
SELECT COUNT(*) FROM pipeline.orders WHERE id = 1;
"
# Expected: 0
```

---

## Step 9: Run Load Test

```python
# scripts/load_test.py
"""Inserts 10,000 orders into MySQL and measures pipeline latency."""
import mysql.connector
import psycopg2
import time
import random

MYSQL_CONFIG = {
    "host": "localhost", "port": 3306,
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

```bash
python3 scripts/load_test.py
```

---

## Step 10: Monitor the Pipeline

```bash
# Watch consumer lag
watch -n 2 'docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe \
  --group connect-postgres-sink'

# Open Grafana dashboard
# http://localhost:3000 (admin/admin)
# Import dashboard ID 7589 for Kafka metrics

# Check connector metrics
curl -s http://localhost:8083/connectors/postgres-sink/status
curl -s http://localhost:8083/connectors/mysql-cdc-source/status
```

---

## Step 11: Simulate Failure and Recovery

### Test 1: MySQL Connection Loss

```bash
# Stop MySQL
docker-compose stop mysql

# Watch connector status (will show FAILED after timeout)
sleep 30
curl -s http://localhost:8083/connectors/mysql-cdc-source/status | python3 -m json.tool

# Restart MySQL
docker-compose start mysql

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
docker-compose stop postgres

# Insert data into MySQL while PG is down
docker exec mysql-source mysql -u kafka_user -pkafka_password sourcedb -e "
INSERT INTO orders (customer_id, product_id, quantity, amount, status)
VALUES (2, 888, 5, 499.99, 'PENDING');
"

# Data is safe in Kafka (durable log)
echo "Data is in Kafka, waiting for PG to come back..."

# Restart PostgreSQL
docker-compose start postgres
sleep 30

# Sink connector auto-recovers and catches up
curl -s http://localhost:8083/connectors/postgres-sink/status

# Verify data appeared
docker exec postgres-target psql -U kafka_user -d targetdb -c "
SELECT * FROM pipeline.orders WHERE amount = 499.99;
"
```

---

## Complete Pipeline Summary

```
┌──────────────────────────────────────────────────────────────────────┐
│  MySQL sourcedb                                                      │
│  ├── orders (id, customer_id, product_id, amount, status, ...)      │
│  └── customers (id, name, email, phone, ...)                        │
└─────────────────────┬────────────────────────────────────────────────┘
                      │ binlog (ROW format)
                      ▼
┌──────────────────────────────────────────────────────────────────────┐
│  Debezium MySQL Source Connector (in Kafka Connect)                  │
│  ├── Snapshot: reads all existing rows on first start               │
│  ├── Stream: tails binlog for all subsequent changes                │
│  ├── Transforms: ExtractNewRecordState (unwrap envelope)             │
│  └── Error handling: DLQ for bad records                            │
└─────────────────────┬────────────────────────────────────────────────┘
                      │ Avro records
                      ▼
┌──────────────────────────────────────────────────────────────────────┐
│  Kafka Cluster                                                       │
│  ├── Topic: prod.mysql.sourcedb.orders   (3 partitions, RF=1)       │
│  ├── Topic: prod.mysql.sourcedb.customers (3 partitions, RF=1)      │
│  └── Topic: prod.dlq.errors (1 partition, retain forever)           │
│                                                                      │
│  Schema Registry: Avro schemas stored and enforced                  │
└─────────────────────┬────────────────────────────────────────────────┘
                      │ Avro records
                      ▼
┌──────────────────────────────────────────────────────────────────────┐
│  JDBC Sink Connector (in Kafka Connect)                              │
│  ├── Mode: UPSERT on primary key (id)                               │
│  ├── Auto-create and auto-evolve target tables                      │
│  ├── Batch size: 3000 records per flush                             │
│  └── Error handling: retry 10x, then DLQ                           │
└─────────────────────┬────────────────────────────────────────────────┘
                      │ JDBC
                      ▼
┌──────────────────────────────────────────────────────────────────────┐
│  PostgreSQL targetdb                                                 │
│  ├── pipeline.orders   (mirror of sourcedb.orders + CDC metadata)  │
│  └── pipeline.customers (mirror of sourcedb.customers)             │
└──────────────────────────────────────────────────────────────────────┘

Performance (single-node dev):
  Snapshot throughput:  ~5,000–20,000 rows/sec
  Streaming latency:    < 500ms (typically 100–200ms)
  Throughput (steady):  ~1,000–5,000 events/sec per topic partition
```

---

## Checklist: Production Readiness

```
Infrastructure
  ✓ Multi-broker Kafka cluster (3+ brokers)
  ✓ Replication factor = 3, min.insync.replicas = 2
  ✓ KRaft controllers (3) separate from brokers
  ✓ Persistent storage (not ephemeral)
  ✓ Resource limits configured

Connectivity
  ✓ TLS encryption enabled
  ✓ SASL authentication configured
  ✓ ACLs defined per service account
  ✓ Secrets in Vault or K8s Secrets (not plaintext)

Data Integrity
  ✓ Schema Registry with BACKWARD compatibility
  ✓ Debezium heartbeat table configured (PostgreSQL)
  ✓ exactly-once semantics or idempotent sinks
  ✓ DLQ topic configured for all connectors
  ✓ Sink uses UPSERT (idempotent)

Observability
  ✓ Prometheus metrics collected
  ✓ Grafana dashboards imported
  ✓ Consumer lag alerts configured
  ✓ Connector failure alerts configured
  ✓ Structured JSON logging

Operations
  ✓ Topic naming convention documented and enforced
  ✓ Connector deployment via CI/CD
  ✓ Runbook for common failures
  ✓ Data recovery procedure documented
  ✓ Regular lag monitoring
  ✓ Backup strategy for offsets and schemas
```
