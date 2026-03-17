# System Architecture

## High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                          DATA SOURCES                                           │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────────────┐ │
│  │  MySQL   │  │PostgreSQL│  │  Oracle  │  │SQL Server│  │  CSV / Flat File │ │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────────┬─────────┘ │
└───────┼─────────────┼─────────────┼──────────────┼─────────────────┼───────────┘
        │             │             │              │                 │
        ▼             ▼             ▼              ▼                 ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         INGESTION LAYER                                         │
│  ┌──────────────────────────────────────────────────────────────────────────┐   │
│  │                      Kafka Connect (Source)                              │   │
│  │  ┌────────────┐  ┌────────────┐  ┌────────────┐  ┌──────────────────┐   │   │
│  │  │ Debezium   │  │ Debezium   │  │ Debezium   │  │  Kafka Connect   │   │   │
│  │  │ MySQL CDC  │  │  PG CDC    │  │Oracle LogM │  │  FileStream/S3   │   │   │
│  │  └────────────┘  └────────────┘  └────────────┘  └──────────────────┘   │   │
│  └──────────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────┬───────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         KAFKA CLUSTER                                           │
│                                                                                 │
│   ┌───────────┐   ┌───────────┐   ┌───────────┐                                │
│   │  Broker 1 │   │  Broker 2 │   │  Broker 3 │  (KRaft / Zookeeper quorum)   │
│   └───────────┘   └───────────┘   └───────────┘                                │
│                                                                                 │
│   Topics:  orders.cdc  users.cdc  inventory.cdc  files.raw  dlq.errors         │
│                                                                                 │
│   ┌─────────────────────────────┐                                               │
│   │      Schema Registry        │  (Confluent / Apicurio)                      │
│   └─────────────────────────────┘                                               │
└─────────────────────────────┬───────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                       PROCESSING LAYER (Optional)                               │
│  ┌──────────────┐  ┌──────────────┐  ┌─────────────────────────────────────┐   │
│  │Kafka Streams │  │    ksqlDB    │  │     Apache Flink / Spark Streaming  │   │
│  │(lightweight) │  │(SQL-based)   │  │     (complex stateful processing)   │   │
│  └──────────────┘  └──────────────┘  └─────────────────────────────────────┘   │
└─────────────────────────────┬───────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         SINK LAYER                                              │
│  ┌──────────────────────────────────────────────────────────────────────────┐   │
│  │                      Kafka Connect (Sink)                                │   │
│  │  ┌────────────┐  ┌────────────┐  ┌────────────┐  ┌──────────────────┐   │   │
│  │  │  JDBC Sink │  │  JDBC Sink │  │  JDBC Sink │  │  Custom Consumer │   │   │
│  │  │ PostgreSQL │  │   MySQL    │  │ SQL Server │  │  / Kafka Streams │   │   │
│  │  └────────────┘  └────────────┘  └────────────┘  └──────────────────┘   │   │
│  └──────────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────┬───────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        DATA TARGETS                                             │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────────────┐ │
│  │PostgreSQL│  │  MySQL   │  │SQL Server│  │  Oracle  │  │  Data Lake / S3  │ │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘  └──────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────────┐
│                    OBSERVABILITY & CONTROL PLANE                                │
│  Prometheus + Grafana │ Kafka UI / Confluent Control Center │ Alertmanager      │
│  Schema Registry UI   │ Kafka Connect REST API              │ Log aggregation   │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## Component Breakdown

### 1. Kafka Cluster
The central nervous system. All data flows through Kafka topics.

| Component | Role |
|-----------|------|
| Brokers | Store and serve partitioned topic data |
| KRaft / Zookeeper | Cluster metadata, leader election |
| Topics | Logical channels (one per table / entity recommended) |
| Partitions | Parallelism unit; more = more throughput |
| Replication | Fault tolerance (RF=3 for production) |

### 2. Schema Registry
Enforces schema contracts between producers and consumers.

- Stores Avro / JSON Schema / Protobuf schemas
- Prevents breaking changes from reaching consumers
- Each topic has a subject: `<topic>-value` and `<topic>-key`

### 3. Kafka Connect
Framework for running source/sink connectors without writing consumer code.

```
REST API → Connector Config → Tasks (worker threads) → Kafka
```

- **Source connectors**: pull data into Kafka
- **Sink connectors**: push data from Kafka to target
- **Workers**: distributed (recommended for production) or standalone

### 4. Debezium (CDC Source)
Reads database transaction logs and emits change events.

```
MySQL binlog / PostgreSQL WAL / Oracle LogMiner
    → Debezium connector
        → Kafka topic per table
```

Change event structure:
```json
{
  "before": { "id": 1, "name": "Alice" },
  "after":  { "id": 1, "name": "Alice B." },
  "op":     "u",
  "ts_ms":  1700000000000,
  "source": { "db": "mydb", "table": "users", "lsn": 12345 }
}
```

`op` values: `c` (create), `u` (update), `d` (delete), `r` (read/snapshot)

### 5. Processing Layer (Optional)

| Tool | Best For |
|------|----------|
| Kafka Streams | Lightweight, in-process, Java/Kotlin |
| ksqlDB | SQL-based stream processing, ad-hoc queries |
| Apache Flink | Complex stateful, large-scale, exactly-once |
| Spark Streaming | Micro-batch, existing Spark ecosystem |

### 6. Kafka Connect Sink
Writes from Kafka topics to target databases. JDBC Sink Connector handles most relational targets.

---

## Data Flow Patterns

### Pattern 1: CDC Replication (MySQL → PostgreSQL)
```
MySQL binlog → Debezium Source → Kafka topic → JDBC Sink → PostgreSQL
```

### Pattern 2: File Ingestion (CSV → PostgreSQL)
```
CSV file → FileStream Source Connector → Kafka topic → JDBC Sink → PostgreSQL
```

### Pattern 3: Cross-DB Migration (Oracle → SQL Server)
```
Oracle LogMiner → Debezium Source → Kafka topic → JDBC Sink → SQL Server
```

### Pattern 4: Fan-Out (One source → Multiple targets)
```
MySQL binlog → Debezium → Kafka topic ─┬→ JDBC Sink → PostgreSQL
                                        ├→ JDBC Sink → MySQL (replica)
                                        └→ S3 Sink   → Data Lake
```

### Pattern 5: Aggregation / Enrichment
```
orders.cdc ─┐
             ├→ Kafka Streams / Flink → enriched.orders → JDBC Sink → DW
users.cdc  ─┘
```

---

## Topic Naming Convention

```
<environment>.<database>.<schema>.<table>

Examples:
  prod.mysql.ecommerce.orders
  prod.postgres.hr.employees
  dev.oracle.finance.invoices
  prod.files.uploads.csv_raw
  prod.dlq.errors
```

---

## Deployment Topology

### Development (Single Node)
```
┌──────────────────────────────────────┐
│  Docker Compose                      │
│  - 1x Kafka (KRaft)                  │
│  - 1x Schema Registry                │
│  - 1x Kafka Connect                  │
│  - 1x Debezium                       │
│  - 1x Kafka UI                       │
└──────────────────────────────────────┘
```

### Production (Multi-Node)
```
┌──────────────────────────────────────┐
│  Kubernetes                          │
│  - 3x Kafka Brokers (StatefulSet)    │
│  - 3x KRaft Controllers              │
│  - 2x Schema Registry                │
│  - 3x Kafka Connect Workers          │
│  - 2x ksqlDB (optional)              │
│  - Prometheus + Grafana              │
└──────────────────────────────────────┘
```
