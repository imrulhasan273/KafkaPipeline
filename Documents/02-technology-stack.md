# Technology Stack

## Core Components

| Component | Recommended Tool | Version | Notes |
|-----------|-----------------|---------|-------|
| Message Broker | Apache Kafka | 3.7+ | Use KRaft mode (no Zookeeper) |
| Schema Registry | Confluent Schema Registry | 7.6+ | Or Apicurio for open-source |
| Source CDC | Debezium | 2.6+ | Runs inside Kafka Connect |
| Connector Framework | Kafka Connect | (bundled with Kafka) | Distributed mode in prod |
| Stream Processing | Kafka Streams / Flink | Kafka 3.7 / Flink 1.19 | Choose by complexity |
| SQL Stream Processing | ksqlDB | 0.29+ | Confluent-maintained |
| Sink | JDBC Sink Connector | Confluent 10.x | Handles most RDBMS |
| Orchestration | Apache Airflow | 2.9+ | For batch workflows |
| Containerization | Docker / Kubernetes | Docker 25+ / K8s 1.29+ | Helm charts available |
| Monitoring | Prometheus + Grafana | Latest | JMX exporter for Kafka |
| Log Aggregation | ELK Stack / Loki | Latest | Structured JSON logging |

---

## Database Driver Matrix

| Source/Target | Driver JAR | JDBC URL Pattern |
|--------------|------------|-----------------|
| MySQL | `mysql-connector-java-8.x.jar` | `jdbc:mysql://host:3306/db` |
| PostgreSQL | `postgresql-42.x.jar` | `jdbc:postgresql://host:5432/db` |
| Oracle | `ojdbc11.jar` | `jdbc:oracle:thin:@host:1521/SID` |
| SQL Server | `mssql-jdbc-12.x.jar` | `jdbc:sqlserver://host:1433;databaseName=db` |

---

## When to Use CDC vs Batch Ingestion

### CDC (Change Data Capture) — Use When:

- You need **near-real-time** data movement (sub-second to seconds)
- Source database has **high write volume** and you need every change
- You require **audit trail** of all inserts, updates, and deletes
- **Incremental replication** is needed (only changed rows)
- Minimizing **source database load** is critical (log reading is lightweight)

**Tools:** Debezium, Maxwell (MySQL), pg_logical (PostgreSQL native)

```
Latency: < 1 second
Source load: Very low (reads binlog/WAL, not tables)
Complexity: Medium (requires binlog/WAL setup)
```

### Batch Ingestion — Use When:

- Data changes **infrequently** (hourly, daily)
- Source lacks CDC capability or log access
- You're doing **initial full loads** (bulk migration)
- Downstream can tolerate **minutes to hours** of latency
- Working with **flat files** (CSV, JSON dumps)

**Tools:** Kafka Connect JDBC Source (polling), Airflow + custom producers, Spark

```
Latency: Minutes to hours
Source load: Higher (SELECT queries on tables)
Complexity: Low to medium
```

### Hybrid Approach (Recommended for Production)

```
Phase 1 — Initial load: Batch snapshot of all existing data
Phase 2 — Ongoing sync: Switch to CDC for continuous changes
```

Debezium handles this automatically with its **snapshot + streaming** mode.

---

## Connector Ecosystem

### Source Connectors

| Connector | Use Case | Provider |
|-----------|----------|----------|
| Debezium MySQL | CDC from MySQL 5.7+ / 8.x | Debezium (Red Hat) |
| Debezium PostgreSQL | CDC from PostgreSQL 9.6+ | Debezium (Red Hat) |
| Debezium Oracle | CDC from Oracle 11g+ (LogMiner) | Debezium (Red Hat) |
| Debezium SQL Server | CDC from SQL Server 2016+ | Debezium (Red Hat) |
| Kafka Connect JDBC Source | Polling-based ingestion | Confluent |
| FileStream Source | Local file ingestion | Apache Kafka (built-in) |
| S3 Source | Read files from S3/MinIO | Confluent / Camel |
| Spooldir Source | CSV/JSON/Avro file ingestion | Jeremy Custenborder |

### Sink Connectors

| Connector | Target | Provider |
|-----------|--------|----------|
| JDBC Sink | PostgreSQL, MySQL, SQL Server, Oracle | Confluent |
| Debezium JDBC Sink | Full CDC-aware upsert sink | Debezium |
| S3 Sink | AWS S3 / MinIO | Confluent |
| Elasticsearch Sink | Elasticsearch / OpenSearch | Confluent |
| BigQuery Sink | Google BigQuery | Google / Confluent |
| Snowflake Sink | Snowflake DWH | Snowflake |
| MongoDB Sink | MongoDB | MongoDB |

---

## Serialization Format Comparison

| Format | Schema Evolution | Human Readable | Performance | Best For |
|--------|-----------------|----------------|-------------|----------|
| **Avro** | Excellent (Registry) | No (binary) | High | Production CDC, high throughput |
| **JSON Schema** | Good (Registry) | Yes | Medium | Debugging, APIs, moderate load |
| **Protobuf** | Excellent (Registry) | No (binary) | Highest | Extreme throughput, multi-language |
| **Plain JSON** | None | Yes | Medium | Development, simple pipelines |

**Recommendation:** Use **Avro** for production CDC pipelines. Use **JSON** during development.

---

## Infrastructure Sizing Guide

### Development (Docker Compose)
```
Kafka:          1 broker, 2 vCPU, 4 GB RAM
Connect:        1 worker, 2 vCPU, 2 GB RAM
Schema Reg:     1 instance, 1 vCPU, 1 GB RAM
Total:          ~4 vCPU, 8 GB RAM
```

### Small Production (< 10k events/sec)
```
Kafka:          3 brokers, 4 vCPU, 16 GB RAM each, 500 GB SSD
Connect:        2 workers, 4 vCPU, 8 GB RAM each
Schema Reg:     2 instances, 2 vCPU, 2 GB RAM each
Total:          ~24 vCPU, ~68 GB RAM
```

### Large Production (> 100k events/sec)
```
Kafka:          6+ brokers, 8 vCPU, 32 GB RAM, 2 TB NVMe SSD
Connect:        5+ workers, 8 vCPU, 16 GB RAM each
Schema Reg:     3 instances behind load balancer
Flink:          Dedicated cluster
Total:          Scale horizontally as needed
```
