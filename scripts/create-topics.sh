#!/bin/bash

KAFKA_CONTAINER="kafka"
BOOTSTRAP="localhost:9092"

create_topic() {
  local topic=$1
  local partitions=${2:-3}
  local replication=${3:-1}
  local retention_ms=${4:-604800000}  # 7 days default

  docker exec $KAFKA_CONTAINER kafka-topics \
    --create \
    --bootstrap-server $BOOTSTRAP \
    --topic "$topic" \
    --partitions $partitions \
    --replication-factor $replication \
    --config retention.ms=$retention_ms \
    --if-not-exists

  echo "Created topic: $topic"
}

# CDC source topics — one per table
create_topic "prod.mysql.sourcedb.orders"     3 1
create_topic "prod.mysql.sourcedb.customers"  3 1
create_topic "prod.mysql.sourcedb.products"   3 1

# Dead Letter Queue — retain forever (-1)
create_topic "prod.dlq.errors" 1 1 -1

# Kafka Connect internal topics (required before connectors can start)
create_topic "_connect-configs"  1 1
create_topic "_connect-offsets" 25 1
create_topic "_connect-status"   5 1

echo ""
echo "All topics created. Current topic list:"
docker exec $KAFKA_CONTAINER kafka-topics \
  --list \
  --bootstrap-server $BOOTSTRAP