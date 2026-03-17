# Kafka-Based ETL/Data Pipeline — Documentation Index

Production-grade Kafka data pipeline system covering ingestion, processing, delivery, and operations.

## Documents

| # | Document | Description |
|---|----------|-------------|
| 01 | [Architecture](01-architecture.md) | System architecture, components, data flow diagrams |
| 02 | [Technology Stack](02-technology-stack.md) | Tools, frameworks, CDC vs batch decisions |
| 03 | [Environment Setup](03-environment-setup.md) | Kafka cluster, Zookeeper/KRaft, Schema Registry |
| 04 | [Data Ingestion](04-data-ingestion.md) | Debezium CDC, file ingestion, source connectors |
| 05 | [Data Processing](05-data-processing.md) | Kafka Streams, ksqlDB, Flink, transformations |
| 06 | [Data Sink](06-data-sink.md) | Sink connectors, writing to PostgreSQL/MySQL/SQL Server |
| 07 | [Schema Management](07-schema-management.md) | Avro/JSON/Protobuf, schema evolution, compatibility |
| 08 | [Error Handling](08-error-handling.md) | DLQ, retry strategies, exactly-once semantics |
| 09 | [Monitoring](09-monitoring.md) | Prometheus, Grafana, lag monitoring, alerting |
| 10 | [Security](10-security.md) | SASL/SSL, ACLs, encryption, secrets management |
| 11 | [Scaling & Performance](11-scaling-performance.md) | Partitioning, throughput tuning, consumer groups |
| 12 | [Deployment](12-deployment.md) | Docker Compose, Kubernetes, CI/CD |
| 13 | [Real-World Example](13-real-world-example.md) | End-to-end MySQL → Kafka → PostgreSQL with CDC |

## Supported Data Movement Scenarios

- MySQL → PostgreSQL
- PostgreSQL → MySQL
- PostgreSQL → PostgreSQL
- MySQL → MySQL
- Oracle → SQL Server
- CSV/Flat Files → Database

## Quick Start

Start with [03-environment-setup.md](03-environment-setup.md) to get a local dev environment running,
then follow [13-real-world-example.md](13-real-world-example.md) for an end-to-end walkthrough.


