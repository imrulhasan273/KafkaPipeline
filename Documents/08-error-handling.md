# Error Handling & Reliability

## Delivery Semantics

| Semantic | Description | Config | Use When |
|----------|-------------|--------|----------|
| **At-most-once** | May lose messages, never duplicates | `acks=0`, no retries | Metrics, non-critical logs |
| **At-least-once** | No message loss, may duplicate | `acks=all`, retries enabled | Most ETL pipelines |
| **Exactly-once** | No loss, no duplicates | `enable.idempotence=true` + transactions | Financial, inventory |

---

## Producer Configuration for Reliability

### `config/producer.properties`

```properties
# Durability
acks=all
enable.idempotence=true
max.in.flight.requests.per.connection=5

# Retries
retries=2147483647
retry.backoff.ms=1000
delivery.timeout.ms=120000

# Batching for throughput
batch.size=65536
linger.ms=5
compression.type=snappy

# Timeouts
request.timeout.ms=30000
```

### Java producer with exactly-once transactions

```java
Properties props = new Properties();
props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
props.put(ProducerConfig.ACKS_CONFIG, "all");
props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "order-producer-tx-1");
props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);

KafkaProducer<String, String> producer = new KafkaProducer<>(props);
producer.initTransactions();

try {
    producer.beginTransaction();

    producer.send(new ProducerRecord<>("orders.processed", key, value));
    producer.send(new ProducerRecord<>("audit.log", key, auditValue));

    producer.commitTransaction();
} catch (ProducerFencedException | OutOfOrderSequenceException e) {
    // Fatal: cannot recover, close producer
    producer.close();
    throw e;
} catch (KafkaException e) {
    // Transient: abort and retry
    producer.abortTransaction();
    throw e;
}
```

---

## Consumer Configuration for Reliability

```properties
# Disable auto-commit — control commits manually
enable.auto.commit=false

# Isolation for exactly-once reads (only read committed transactions)
isolation.level=read_committed

# Session management
session.timeout.ms=30000
heartbeat.interval.ms=10000
max.poll.interval.ms=300000
max.poll.records=500

# Offset reset (only applies if no committed offset exists)
auto.offset.reset=earliest
```

### Manual commit pattern

```python
consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "my-consumer-group",
    "enable.auto.commit": False,
    "auto.offset.reset": "earliest",
    "isolation.level": "read_committed",
})

try:
    while True:
        messages = consumer.consume(num_messages=100, timeout=1.0)
        if not messages:
            continue

        # Process batch
        for msg in messages:
            if msg.error():
                handle_error(msg.error())
                continue
            process(msg)

        # Commit only after successful processing
        consumer.commit(asynchronous=False)

except Exception as e:
    logger.error(f"Consumer error: {e}")
finally:
    consumer.close()
```

---

## Dead Letter Queue (DLQ)

Messages that fail processing are sent to a DLQ topic for investigation and reprocessing.

### Kafka Connect DLQ Configuration

```json
{
  "errors.tolerance": "all",
  "errors.log.enable": "true",
  "errors.log.include.messages": "true",
  "errors.retry.timeout": "300000",
  "errors.retry.delay.max.ms": "60000",
  "errors.deadletterqueue.topic.name": "prod.dlq.errors",
  "errors.deadletterqueue.topic.replication.factor": "3",
  "errors.deadletterqueue.context.headers.enable": "true"
}
```

DLQ message headers (added automatically):
```
__connect.errors.topic                 = prod.mysql.sourcedb.orders
__connect.errors.partition             = 2
__connect.errors.offset                = 12345
__connect.errors.connector.name        = postgres-sink
__connect.errors.task.id               = 0
__connect.errors.stage                 = VALUE_CONVERTER
__connect.errors.class.name            = org.apache.kafka.connect.errors.DataException
__connect.errors.message               = Failed to deserialize data...
__connect.errors.exception.stacktrace  = ...
```

### DLQ Consumer / Reprocessor

```python
# scripts/dlq_reprocessor.py
"""
Read DLQ messages, fix them, and republish to the original topic.
"""
import json
from confluent_kafka import Consumer, Producer

DLQ_TOPIC = "prod.dlq.errors"
consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "dlq-inspector",
    "auto.offset.reset": "earliest",
})
producer = Producer({"bootstrap.servers": "localhost:9092"})

consumer.subscribe([DLQ_TOPIC])

try:
    while True:
        msg = consumer.poll(timeout=5.0)
        if msg is None:
            break
        if msg.error():
            print(f"Error: {msg.error()}")
            continue

        # Extract metadata from headers
        headers = dict(msg.headers() or [])
        original_topic = headers.get("__connect.errors.topic", b"").decode()
        error_msg = headers.get("__connect.errors.message", b"").decode()

        print(f"DLQ Message from {original_topic}: {error_msg}")
        print(f"Value: {msg.value()}")

        # Manual fix logic here
        # fixed_value = fix_record(msg.value())
        # producer.produce(original_topic, key=msg.key(), value=fixed_value)

finally:
    consumer.close()
```

---

## Retry Strategies

### Exponential Backoff in Custom Consumer

```python
import time
import random

def with_retry(fn, max_retries=5, base_delay=1.0, max_delay=60.0):
    """Execute fn with exponential backoff retry."""
    for attempt in range(max_retries):
        try:
            return fn()
        except TransientError as e:
            if attempt == max_retries - 1:
                raise
            delay = min(base_delay * (2 ** attempt) + random.uniform(0, 1), max_delay)
            logger.warning(f"Attempt {attempt + 1} failed: {e}. Retrying in {delay:.2f}s")
            time.sleep(delay)
```

### Kafka Connect Retry Config

```json
{
  "max.retries": "10",
  "retry.backoff.ms": "3000",
  "errors.retry.timeout": "300000",
  "errors.retry.delay.max.ms": "60000"
}
```

---

## Exactly-Once in Kafka Streams

```java
Properties props = new Properties();
props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
// EXACTLY_ONCE_V2 is more performant than EXACTLY_ONCE (deprecated)
// It uses epoch-based fencing instead of transactions
```

---

## Offset Management

### Reset consumer group offsets (use with caution)

```bash
# Preview what would change (dry run)
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group connect-postgres-sink \
  --reset-offsets \
  --to-earliest \
  --topic prod.mysql.sourcedb.orders \
  --dry-run

# Actually reset (stop consumers first!)
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group connect-postgres-sink \
  --reset-offsets \
  --to-earliest \
  --topic prod.mysql.sourcedb.orders \
  --execute

# Reset to specific datetime
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group connect-postgres-sink \
  --reset-offsets \
  --to-datetime 2024-01-15T00:00:00.000 \
  --topic prod.mysql.sourcedb.orders \
  --execute

# Reset to specific offset
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group connect-postgres-sink \
  --reset-offsets \
  --to-offset 5000 \
  --topic prod.mysql.sourcedb.orders:0 \
  --execute
```

---

## Idempotent Sink Pattern

Always design sinks to be idempotent — processing the same message twice should produce the same result.

### PostgreSQL UPSERT (idempotent)

```sql
INSERT INTO pipeline.orders (id, customer_id, amount, status, updated_at)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (id) DO UPDATE SET
    customer_id = EXCLUDED.customer_id,
    amount      = EXCLUDED.amount,
    status      = EXCLUDED.status,
    updated_at  = EXCLUDED.updated_at
WHERE pipeline.orders.updated_at < EXCLUDED.updated_at;
-- The WHERE clause prevents older events from overwriting newer ones
```

### MySQL UPSERT (idempotent)

```sql
INSERT INTO orders (id, customer_id, amount, status, updated_at)
VALUES (?, ?, ?, ?, ?)
ON DUPLICATE KEY UPDATE
    customer_id = VALUES(customer_id),
    amount      = VALUES(amount),
    status      = VALUES(status),
    updated_at  = IF(updated_at < VALUES(updated_at), VALUES(updated_at), updated_at);
```

---

## Error Handling Flow Diagram

```
Message arrives at Consumer/Sink
         │
         ▼
  Deserialize message
         │
    ┌────┴────┐
    │ Success │         │ Failure (bad schema)
    └────┬────┘         └────────────────────→ DLQ (skip message)
         │
         ▼
  Apply transformation
         │
    ┌────┴────┐
    │ Success │         │ Failure (transform error)
    └────┬────┘         └────────────────────→ DLQ (skip message)
         │
         ▼
   Write to target DB
         │
    ┌────┴────┐
    │ Success │         │ Failure (transient DB error)
    └────┬────┘         └────────────────────→ Retry with backoff
         │                                            │
         ▼                                    Max retries exceeded
   Commit offset                                      │
                                                      ▼
                                                   DLQ + Alert
```
