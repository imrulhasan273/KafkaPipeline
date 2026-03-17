# Data Processing

## Processing Options Overview

| Tool | Type | Language | State | Complexity | Best For |
|------|------|----------|-------|------------|----------|
| Kafka Streams | Library | Java/Kotlin | Yes | Low-Medium | In-app processing |
| ksqlDB | SQL Engine | SQL | Yes | Low | Ad-hoc, dashboards |
| Apache Flink | Framework | Java/Python/SQL | Yes | High | Large-scale, exactly-once |
| Spark Streaming | Framework | Java/Python/Scala | Yes | High | Batch + stream hybrid |

---

## 1. Kafka Streams

Lightweight stream processing library embedded in your application — no separate cluster needed.

### Dependency (`pom.xml`)

```xml
<dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>kafka-streams</artifactId>
    <version>3.7.0</version>
</dependency>
<dependency>
    <groupId>io.confluent</groupId>
    <artifactId>kafka-streams-avro-serde</artifactId>
    <version>7.6.1</version>
</dependency>
```

### Example: Filter + Enrich + Route

```java
// OrderEnrichmentPipeline.java
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;

import java.util.Properties;

public class OrderEnrichmentPipeline {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "order-enrichment-v1");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
        props.put("schema.registry.url", "http://localhost:8081");
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);

        StreamsBuilder builder = new StreamsBuilder();

        // Source stream from CDC topic
        KStream<String, GenericRecord> orders =
            builder.stream("prod.mysql.sourcedb.orders");

        // Route deletes to separate topic
        orders
            .filter((key, value) -> "d".equals(value.get("__op").toString()))
            .to("prod.mysql.sourcedb.orders.deletes");

        // Process inserts and updates
        KStream<String, GenericRecord> activeOrders = orders
            .filter((key, value) -> {
                String op = value.get("__op").toString();
                return "c".equals(op) || "u".equals(op);
            });

        // Filter high-value orders
        activeOrders
            .filter((key, value) -> {
                double amount = Double.parseDouble(value.get("amount").toString());
                return amount > 500.0;
            })
            .to("prod.orders.high-value");

        // Group and count by status
        activeOrders
            .groupBy((key, value) -> value.get("status").toString())
            .count(Materialized.as("orders-by-status-store"))
            .toStream()
            .to("prod.orders.status-counts");

        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        // Graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        streams.start();
        System.out.println("Order enrichment pipeline started.");
    }
}
```

### Stream-Stream Join (Enrich Orders with Customers)

```java
// Join orders with customer data
KTable<String, GenericRecord> customers =
    builder.table("prod.mysql.sourcedb.customers");

KStream<String, GenericRecord> orders =
    builder.stream("prod.mysql.sourcedb.orders");

// Re-key orders by customer_id for joining
KStream<String, GenericRecord> ordersByCustomer = orders
    .selectKey((key, value) -> value.get("customer_id").toString());

// Join
KStream<String, String> enriched = ordersByCustomer.join(
    customers,
    (order, customer) -> {
        // Combine into enriched JSON
        return String.format(
            "{\"order_id\":%s,\"customer_name\":\"%s\",\"amount\":%s,\"status\":\"%s\"}",
            order.get("id"),
            customer != null ? customer.get("name") : "UNKNOWN",
            order.get("amount"),
            order.get("status")
        );
    }
);

enriched.to("prod.orders.enriched",
    Produced.with(Serdes.String(), Serdes.String()));
```

### Windowed Aggregation

```java
// Count orders per customer in 1-hour windows
orders
    .selectKey((k, v) -> v.get("customer_id").toString())
    .groupByKey()
    .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofHours(1)))
    .count()
    .toStream()
    .map((windowedKey, count) -> KeyValue.pair(
        windowedKey.key(),
        String.format("{\"customer_id\":\"%s\",\"order_count\":%d,\"window_start\":%d}",
            windowedKey.key(), count, windowedKey.window().start())
    ))
    .to("prod.orders.hourly-counts");
```

---

## 2. ksqlDB

SQL-based stream processing. Runs as a separate cluster. Ideal for analytics and quick transformations.

### Start ksqlDB in Docker Compose

```yaml
  ksqldb-server:
    image: confluentinc/ksqldb-server:0.29.0
    hostname: ksqldb-server
    depends_on:
      - kafka
      - schema-registry
    ports:
      - "8088:8088"
    environment:
      KSQL_BOOTSTRAP_SERVERS: "kafka:9092"
      KSQL_LISTENERS: "http://0.0.0.0:8088"
      KSQL_KSQL_SCHEMA_REGISTRY_URL: "http://schema-registry:8081"
      KSQL_KSQL_LOGGING_PROCESSING_STREAM_AUTO_CREATE: "true"
      KSQL_KSQL_LOGGING_PROCESSING_TOPIC_AUTO_CREATE: "true"
      KSQL_KSQL_SERVICE_ID: "pipeline_ksql"
      KSQL_KSQL_STREAMS_PROCESSING_GUARANTEE: "exactly_once_v2"

  ksqldb-cli:
    image: confluentinc/ksqldb-cli:0.29.0
    depends_on:
      - ksqldb-server
    entrypoint: /bin/sh
    tty: true
```

### ksqlDB Queries

```sql
-- Connect to ksqlDB CLI
docker exec -it ksqldb-cli ksql http://ksqldb-server:8088

-- Set offset to beginning for dev
SET 'auto.offset.reset' = 'earliest';

-- Create stream from CDC topic
CREATE STREAM orders_raw (
    id          BIGINT,
    customer_id BIGINT,
    product_id  BIGINT,
    quantity    INT,
    amount      DOUBLE,
    status      VARCHAR,
    created_at  VARCHAR,
    __op        VARCHAR,
    __ts_ms     BIGINT
) WITH (
    KAFKA_TOPIC = 'prod.mysql.sourcedb.orders',
    VALUE_FORMAT = 'AVRO',
    TIMESTAMP = '__ts_ms'
);

-- Filter deletes into separate stream
CREATE STREAM orders_deletes AS
    SELECT * FROM orders_raw
    WHERE __op = 'd'
    EMIT CHANGES;

-- Filter active orders (inserts/updates)
CREATE STREAM orders_active AS
    SELECT * FROM orders_raw
    WHERE __op IN ('c', 'u')
    EMIT CHANGES;

-- Create customer lookup table (KTable from CDC)
CREATE TABLE customers_table (
    id   BIGINT PRIMARY KEY,
    name VARCHAR,
    email VARCHAR
) WITH (
    KAFKA_TOPIC = 'prod.mysql.sourcedb.customers',
    VALUE_FORMAT = 'AVRO'
);

-- Enriched stream: join orders with customers
CREATE STREAM orders_enriched AS
    SELECT
        o.id          AS order_id,
        o.amount,
        o.status,
        o.quantity,
        c.name        AS customer_name,
        c.email       AS customer_email,
        o.__op        AS cdc_op,
        o.__ts_ms     AS event_ts
    FROM orders_active o
    LEFT JOIN customers_table c ON o.customer_id = c.id
    EMIT CHANGES;

-- Aggregate: total revenue per status (last 1 hour)
CREATE TABLE revenue_by_status AS
    SELECT
        status,
        SUM(amount)   AS total_revenue,
        COUNT(*)      AS order_count
    FROM orders_active
    WINDOW TUMBLING (SIZE 1 HOUR)
    GROUP BY status
    EMIT CHANGES;

-- Push query (real-time stream)
SELECT * FROM orders_enriched EMIT CHANGES LIMIT 10;

-- Pull query (current state of table)
SELECT * FROM revenue_by_status WHERE status = 'COMPLETED';
```

---

## 3. Apache Flink

For complex stateful processing, large scale, and guaranteed exactly-once semantics across multiple systems.

### Flink in Docker Compose

```yaml
  flink-jobmanager:
    image: flink:1.19-java17
    ports:
      - "8082:8081"
    command: jobmanager
    environment:
      FLINK_PROPERTIES: |
        jobmanager.rpc.address: flink-jobmanager
        state.backend: rocksdb
        state.checkpoints.dir: file:///flink-checkpoints
        execution.checkpointing.interval: 30s
        execution.checkpointing.mode: EXACTLY_ONCE
    volumes:
      - flink-checkpoints:/flink-checkpoints

  flink-taskmanager:
    image: flink:1.19-java17
    command: taskmanager
    scale: 2
    environment:
      FLINK_PROPERTIES: |
        jobmanager.rpc.address: flink-jobmanager
        taskmanager.numberOfTaskSlots: 4
    volumes:
      - flink-checkpoints:/flink-checkpoints

volumes:
  flink-checkpoints:
```

### Flink SQL: Kafka CDC Processing

```sql
-- Flink SQL CLI or application

-- Source table (reading from Kafka CDC topic)
CREATE TABLE orders_source (
    id          BIGINT,
    customer_id BIGINT,
    amount      DECIMAL(10,2),
    status      STRING,
    event_ts    TIMESTAMP(3),
    __op        STRING,
    WATERMARK FOR event_ts AS event_ts - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'prod.mysql.sourcedb.orders',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-orders-consumer',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'avro-confluent',
    'avro-confluent.url' = 'http://schema-registry:8081'
);

-- Sink table (writing to PostgreSQL)
CREATE TABLE orders_sink (
    id          BIGINT PRIMARY KEY NOT ENFORCED,
    customer_id BIGINT,
    amount      DECIMAL(10,2),
    status      STRING,
    event_ts    TIMESTAMP(3),
    cdc_op      STRING
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/targetdb',
    'table-name' = 'pipeline.orders',
    'username' = 'kafka_user',
    'password' = 'kafka_password',
    'sink.buffer-flush.max-rows' = '1000',
    'sink.buffer-flush.interval' = '2s'
);

-- Insert filtered/transformed rows
INSERT INTO orders_sink
SELECT
    id,
    customer_id,
    amount,
    status,
    event_ts,
    __op AS cdc_op
FROM orders_source
WHERE __op IN ('c', 'u');

-- Windowed aggregation sink
CREATE TABLE hourly_revenue (
    window_start TIMESTAMP(3),
    window_end   TIMESTAMP(3),
    status       STRING,
    total_amount DECIMAL(15,2),
    order_count  BIGINT,
    PRIMARY KEY (window_start, status) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/targetdb',
    'table-name' = 'analytics.hourly_revenue',
    'username' = 'kafka_user',
    'password' = 'kafka_password'
);

INSERT INTO hourly_revenue
SELECT
    TUMBLE_START(event_ts, INTERVAL '1' HOUR) AS window_start,
    TUMBLE_END(event_ts, INTERVAL '1' HOUR)   AS window_end,
    status,
    SUM(amount)  AS total_amount,
    COUNT(*)     AS order_count
FROM orders_source
WHERE __op IN ('c', 'u')
GROUP BY TUMBLE(event_ts, INTERVAL '1' HOUR), status;
```

---

## 4. Kafka Connect Transforms (SMT)

Single Message Transforms — lightweight, no-code transformations within the connector itself.

### Common SMTs

```json
{
  "transforms": "route,cast,mask,timestamp",

  "transforms.route.type": "org.apache.kafka.connect.transforms.ReplaceField$Value",
  "transforms.route.exclude": "internal_field,secret_field",

  "transforms.cast.type": "org.apache.kafka.connect.transforms.Cast$Value",
  "transforms.cast.spec": "amount:float64,quantity:int32",

  "transforms.mask.type": "org.apache.kafka.connect.transforms.MaskField$Value",
  "transforms.mask.fields": "email,phone",
  "transforms.mask.replacement": "***MASKED***",

  "transforms.timestamp.type": "org.apache.kafka.connect.transforms.TimestampConverter$Value",
  "transforms.timestamp.field": "created_at",
  "transforms.timestamp.target.type": "Timestamp",
  "transforms.timestamp.format": "yyyy-MM-dd HH:mm:ss"
}
```

### Topic Routing SMT

```json
{
  "transforms": "route",
  "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
  "transforms.route.regex": "prod\\.mysql\\.(.+)",
  "transforms.route.replacement": "sink.$1"
}
```
