# Scaling & Performance

## Partitioning Strategy

Partitions are the unit of parallelism in Kafka. More partitions = more throughput, but more overhead.

### Partition Count Guidelines

```
Throughput target (MB/s)
─────────────────────────────────────────────
1 partition  ≈  10–50 MB/s throughput
Rule of thumb: partitions = max(consumers, throughput_MB_s / 10)

Examples:
  Small table (< 10k events/day):   3 partitions
  Medium table (< 1M events/day):   6 partitions
  Large table (> 10M events/day):   12–24 partitions
  High-throughput (100M+/day):      48+ partitions
```

### Partitioning by Business Key

```bash
# Create topic with custom partition count
docker exec kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --create \
  --topic prod.mysql.sourcedb.orders \
  --partitions 12 \
  --replication-factor 3 \
  --config min.insync.replicas=2 \
  --config retention.ms=604800000 \
  --config compression.type=snappy
```

### Custom Partitioner (Java)

```java
// Route orders to partitions based on customer_id
// Ensures all events for same customer go to same partition → in-order processing
public class CustomerPartitioner implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes,
                         Object value, byte[] valueBytes, Cluster cluster) {
        int numPartitions = cluster.partitionCountForTopic(topic);
        // key = customer_id
        return Math.abs(key.hashCode()) % numPartitions;
    }
}

// Register in producer config:
// props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomerPartitioner.class);
```

---

## Broker Performance Tuning

### `config/kafka/server.properties`

```properties
# Network
num.network.threads=8
num.io.threads=16
socket.send.buffer.bytes=1048576       # 1 MB
socket.receive.buffer.bytes=1048576    # 1 MB
socket.request.max.bytes=104857600     # 100 MB

# Log (storage)
log.dirs=/data/kafka
num.partitions=6
num.recovery.threads.per.data.dir=4
default.replication.factor=3
min.insync.replicas=2
log.retention.hours=168
log.segment.bytes=1073741824           # 1 GB
log.retention.check.interval.ms=300000

# Compression
compression.type=snappy

# Flush policy (OS-managed is generally better for performance)
log.flush.interval.messages=50000
log.flush.interval.ms=1000

# Replication
replica.lag.time.max.ms=30000
replica.fetch.max.bytes=10485760       # 10 MB

# Controller
controller.socket.timeout.ms=30000
leader.imbalance.check.interval.seconds=300

# Quotas (per-client rate limits)
quota.producer.default=10485760        # 10 MB/s per producer
quota.consumer.default=10485760        # 10 MB/s per consumer
```

---

## Producer Tuning

```properties
# Throughput-optimized producer
acks=1                              # Relax to acks=1 for non-critical high-volume
batch.size=131072                   # 128 KB
linger.ms=20                        # Wait up to 20ms to fill batches
compression.type=snappy             # Good balance of speed and ratio
buffer.memory=67108864              # 64 MB
max.in.flight.requests.per.connection=5

# Latency-optimized producer
acks=all
batch.size=16384
linger.ms=0
compression.type=none
```

---

## Consumer Tuning

```properties
# Throughput-optimized consumer
fetch.min.bytes=65536               # Wait for 64 KB before returning
fetch.max.wait.ms=500               # Or wait up to 500ms
max.partition.fetch.bytes=10485760  # 10 MB per partition per fetch
max.poll.records=2000               # Larger batches
fetch.max.bytes=52428800            # 50 MB per fetch

# Latency-optimized consumer
fetch.min.bytes=1
fetch.max.wait.ms=10
max.poll.records=100
```

---

## Kafka Connect Worker Tuning

### `config/connect/connect-distributed.properties`

```properties
bootstrap.servers=kafka-broker-1:9092,kafka-broker-2:9092,kafka-broker-3:9092
group.id=kafka-connect-cluster

# Internal topic replication
config.storage.replication.factor=3
offset.storage.replication.factor=3
status.storage.replication.factor=3
offset.storage.partitions=25
status.storage.partitions=5

# Worker-level throughput
offset.flush.interval.ms=10000
offset.flush.timeout.ms=5000

# Limits per worker
task.shutdown.graceful.timeout.ms=10000
rest.advertised.host.name=kafka-connect
rest.port=8083

# Internal converter (for offset/config storage)
internal.key.converter=org.apache.kafka.connect.json.JsonConverter
internal.value.converter=org.apache.kafka.connect.json.JsonConverter
internal.key.converter.schemas.enable=false
internal.value.converter.schemas.enable=false

# JVM heap (set via KAFKA_HEAP_OPTS env var)
# KAFKA_HEAP_OPTS=-Xms2g -Xmx4g
```

---

## Consumer Group Scaling

```
Topic: prod.mysql.sourcedb.orders (12 partitions)

Consumer Group: connect-postgres-sink

Scaling rules:
  tasks.max ≤ num_partitions
  Each task reads from one or more partitions

  1 task  → 1 consumer → 12 partitions / 1 = 12 partitions per consumer
  3 tasks → 3 consumers → 12 / 3 = 4 partitions per consumer
  12 tasks → 12 consumers → 1 partition per consumer (max parallelism)
  13 tasks → 12 active + 1 idle (wasted)
```

```json
{
  "tasks.max": "12",   // Match partition count for max throughput
  "batch.size": "5000"
}
```

---

## JVM Tuning for Kafka

```bash
# Kafka Broker JVM flags
export KAFKA_HEAP_OPTS="-Xms6g -Xmx6g"
export KAFKA_JVM_PERFORMANCE_OPTS="
  -server
  -XX:+UseG1GC
  -XX:MaxGCPauseMillis=20
  -XX:InitiatingHeapOccupancyPercent=35
  -XX:+ExplicitGCInvokesConcurrent
  -XX:MaxInlineLevel=15
  -Djava.awt.headless=true"

# Kafka Connect JVM flags
export KAFKA_HEAP_OPTS="-Xms2g -Xmx4g"
export KAFKA_JVM_PERFORMANCE_OPTS="
  -server
  -XX:+UseG1GC
  -XX:MaxGCPauseMillis=100"
```

---

## Topic Compaction (for CDC Tables)

Use log compaction when you only need the latest value per key (like a database table).

```bash
docker exec kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --alter \
  --topic prod.mysql.sourcedb.customers \
  --config cleanup.policy=compact \
  --config min.cleanable.dirty.ratio=0.1 \
  --config segment.ms=86400000 \       # Compact after 1 day
  --config delete.retention.ms=86400000  # Keep tombstones for 1 day
```

With compaction + `ExtractNewRecordState` (delete handling = tombstone):
- Updates → new value overwrites old
- Deletes → tombstone record, then cleaned up after `delete.retention.ms`
- Latest state per key is always retained (like a KTable)

---

## Performance Benchmarking

```bash
# Producer throughput benchmark
docker exec kafka kafka-producer-perf-test \
  --topic benchmark-topic \
  --num-records 10000000 \
  --record-size 1024 \
  --throughput -1 \
  --producer-props \
    bootstrap.servers=localhost:9092 \
    acks=all \
    compression.type=snappy \
    batch.size=131072 \
    linger.ms=20

# Consumer throughput benchmark
docker exec kafka kafka-consumer-perf-test \
  --bootstrap-server localhost:9092 \
  --topic benchmark-topic \
  --messages 10000000 \
  --threads 4

# E2E latency benchmark
docker exec kafka kafka-e2e-latency \
  --broker-list localhost:9092 \
  --topic benchmark-topic \
  --num-records 10000 \
  --record-size 1024 \
  --consumer-fetch-max-wait 0
```

---

## Scaling Decision Matrix

| Symptom | Likely Cause | Solution |
|---------|-------------|----------|
| Consumer lag growing | Too few consumers or slow processing | Increase `tasks.max`, add Connect workers |
| Broker CPU high | Too many small requests | Increase `batch.size`, `linger.ms` |
| Producer latency high | Small batches, `linger.ms=0` | Increase `batch.size=131072`, `linger.ms=20` |
| Disk filling fast | Log retention too long | Decrease `log.retention.hours`, add brokers |
| Rebalance storms | Many consumers, unstable connections | Increase `session.timeout.ms`, use static membership |
| Leader imbalance | Broker restart not re-elected | Run `kafka-leader-election --type preferred` |
| OOM on broker | Heap too small | Increase `-Xmx`, reduce `replica.fetch.max.bytes` |
