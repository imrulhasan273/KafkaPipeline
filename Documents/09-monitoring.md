# Monitoring & Observability

## What to Monitor

| Layer | Key Metrics |
|-------|-------------|
| Kafka Broker | Under-replicated partitions, ISR shrink rate, disk usage, request rate |
| Producer | Send rate, error rate, avg latency, batch size |
| Consumer | Consumer lag, poll rate, commit rate, rebalance frequency |
| Kafka Connect | Task status, error rate, offset lag, throughput |
| JVM | GC pause time, heap usage, thread count |

---

## 1. Prometheus + JMX Exporter Setup

### `monitoring/prometheus.yml`

```yaml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  - job_name: "kafka"
    static_configs:
      - targets: ["kafka:7071"]
    metrics_path: /metrics

  - job_name: "kafka-connect"
    static_configs:
      - targets: ["kafka-connect:7072"]
    metrics_path: /metrics

  - job_name: "schema-registry"
    static_configs:
      - targets: ["schema-registry:7073"]
    metrics_path: /metrics

  - job_name: "kafka-exporter"
    static_configs:
      - targets: ["kafka-exporter:9308"]

alerting:
  alertmanagers:
    - static_configs:
        - targets: ["alertmanager:9093"]

rule_files:
  - /etc/prometheus/rules/kafka-alerts.yml
```

### JMX Exporter Config for Kafka

Create `monitoring/jmx_exporter_config.yml`:

```yaml
lowercaseOutputName: true
lowercaseOutputLabelNames: true

rules:
  # Broker metrics
  - pattern: 'kafka.server<type=BrokerTopicMetrics, name=MessagesInPerSec><>Count'
    name: kafka_broker_messages_in_total
    type: COUNTER

  - pattern: 'kafka.server<type=BrokerTopicMetrics, name=BytesInPerSec><>Count'
    name: kafka_broker_bytes_in_total
    type: COUNTER

  - pattern: 'kafka.server<type=BrokerTopicMetrics, name=BytesOutPerSec><>Count'
    name: kafka_broker_bytes_out_total
    type: COUNTER

  - pattern: 'kafka.server<type=ReplicaManager, name=UnderReplicatedPartitions><>Value'
    name: kafka_broker_under_replicated_partitions
    type: GAUGE

  - pattern: 'kafka.server<type=ReplicaManager, name=OfflinePartitionsCount><>Value'
    name: kafka_broker_offline_partitions
    type: GAUGE

  # Request metrics
  - pattern: 'kafka.network<type=RequestMetrics, name=RequestsPerSec, request=(.+)><>Count'
    name: kafka_network_requests_total
    labels:
      request: "$1"
    type: COUNTER

  # Log flush
  - pattern: 'kafka.log<type=LogFlushStats, name=LogFlushRateAndTimeMs><>Count'
    name: kafka_log_flush_total
    type: COUNTER

  # Consumer group lag (via kafka-exporter below)
  - pattern: 'kafka.consumer<type=consumer-fetch-manager-metrics, client-id=(.+)><>records-lag-max'
    name: kafka_consumer_max_lag
    labels:
      client_id: "$1"
    type: GAUGE
```

### Kafka Exporter (for consumer lag)

Add to `docker-compose.yml`:

```yaml
  kafka-exporter:
    image: danielqsj/kafka-exporter:latest
    command:
      - "--kafka.server=kafka:9092"
      - "--web.listen-address=:9308"
      - "--topic.filter=.*"
      - "--group.filter=.*"
    ports:
      - "9308:9308"
    depends_on:
      - kafka
```

---

## 2. Key Prometheus Queries

```promql
# Consumer lag per group/topic/partition
kafka_consumergroup_lag{consumergroup="connect-postgres-sink"}

# Total lag per consumer group (sum across all partitions)
sum by (consumergroup, topic) (kafka_consumergroup_lag)

# Broker message throughput (msg/s)
rate(kafka_broker_messages_in_total[5m])

# Under-replicated partitions (should be 0)
kafka_broker_under_replicated_partitions

# Offline partitions (must be 0)
kafka_broker_offline_partitions

# Kafka Connect task failures
kafka_connect_connector_task_state{state="failed"}

# Producer error rate
rate(kafka_producer_topic_metrics_record_error_total[5m])

# JVM heap usage
jvm_memory_bytes_used{area="heap"} / jvm_memory_bytes_max{area="heap"}

# GC pause time
rate(jvm_gc_collection_seconds_sum[5m])
```

---

## 3. Alertmanager Rules

### `monitoring/rules/kafka-alerts.yml`

```yaml
groups:
  - name: kafka-alerts
    rules:

      - alert: KafkaUnderReplicatedPartitions
        expr: kafka_broker_under_replicated_partitions > 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Kafka has under-replicated partitions"
          description: "{{ $value }} partitions are under-replicated on broker {{ $labels.instance }}"

      - alert: KafkaOfflinePartitions
        expr: kafka_broker_offline_partitions > 0
        for: 0m
        labels:
          severity: critical
        annotations:
          summary: "Kafka has offline partitions"
          description: "{{ $value }} partitions are OFFLINE"

      - alert: KafkaConsumerLagHigh
        expr: sum by (consumergroup, topic) (kafka_consumergroup_lag) > 10000
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High consumer lag"
          description: "Consumer group {{ $labels.consumergroup }} has lag {{ $value }} on topic {{ $labels.topic }}"

      - alert: KafkaConsumerLagCritical
        expr: sum by (consumergroup, topic) (kafka_consumergroup_lag) > 100000
        for: 2m
        labels:
          severity: critical
        annotations:
          summary: "Critical consumer lag"
          description: "Consumer group {{ $labels.consumergroup }} lag is {{ $value }}"

      - alert: KafkaConnectTaskFailed
        expr: kafka_connect_connector_task_state{state="failed"} == 1
        for: 0m
        labels:
          severity: critical
        annotations:
          summary: "Kafka Connect task failed"
          description: "Connector {{ $labels.connector }} task {{ $labels.task }} has failed"

      - alert: KafkaBrokerDown
        expr: up{job="kafka"} == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Kafka broker is down"
          description: "Kafka broker {{ $labels.instance }} is unreachable"

      - alert: KafkaDiskUsageHigh
        expr: (node_filesystem_size_bytes{mountpoint="/var/lib/kafka"} - node_filesystem_free_bytes{mountpoint="/var/lib/kafka"}) / node_filesystem_size_bytes{mountpoint="/var/lib/kafka"} > 0.80
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Kafka disk usage above 80%"
          description: "Kafka data disk is {{ $value | humanizePercentage }} full"

      - alert: JVMHeapHigh
        expr: jvm_memory_bytes_used{area="heap"} / jvm_memory_bytes_max{area="heap"} > 0.85
        for: 2m
        labels:
          severity: warning
        annotations:
          summary: "JVM heap usage is high"
          description: "{{ $labels.instance }} heap is {{ $value | humanizePercentage }} full"
```

### `monitoring/alertmanager.yml`

```yaml
global:
  resolve_timeout: 5m
  slack_api_url: 'https://hooks.slack.com/services/YOUR/WEBHOOK/URL'

route:
  group_by: ['alertname', 'severity']
  group_wait: 10s
  group_interval: 5m
  repeat_interval: 1h
  receiver: 'default'
  routes:
    - match:
        severity: critical
      receiver: 'critical-alerts'

receivers:
  - name: 'default'
    slack_configs:
      - channel: '#kafka-alerts'
        send_resolved: true
        title: '[{{ .Status | toUpper }}] {{ .GroupLabels.alertname }}'
        text: '{{ range .Alerts }}{{ .Annotations.description }}{{ end }}'

  - name: 'critical-alerts'
    pagerduty_configs:
      - service_key: 'YOUR_PAGERDUTY_KEY'
    slack_configs:
      - channel: '#kafka-critical'
        send_resolved: true
```

---

## 4. Grafana Dashboards

Add to `docker-compose.yml`:

```yaml
  alertmanager:
    image: prom/alertmanager:latest
    ports:
      - "9093:9093"
    volumes:
      - ./monitoring/alertmanager.yml:/etc/alertmanager/alertmanager.yml
    command:
      - '--config.file=/etc/alertmanager/alertmanager.yml'
```

### Dashboard JSON (Key Panels)

Import these dashboards from Grafana.com:
- **Kafka Overview**: Dashboard ID `7589` (from kafka-exporter)
- **Kafka Connect**: Dashboard ID `11173`
- **JVM Overview**: Dashboard ID `8563`
- **Kafka Lag Exporter**: Dashboard ID `14012`

### Custom Dashboard: Consumer Lag

`monitoring/grafana/dashboards/consumer-lag.json` (key panels):

```json
{
  "title": "Kafka Consumer Lag",
  "panels": [
    {
      "title": "Consumer Lag by Group",
      "type": "graph",
      "targets": [
        {
          "expr": "sum by (consumergroup, topic) (kafka_consumergroup_lag)",
          "legendFormat": "{{consumergroup}} / {{topic}}"
        }
      ]
    },
    {
      "title": "Lag Heatmap",
      "type": "heatmap",
      "targets": [
        {
          "expr": "kafka_consumergroup_lag",
          "legendFormat": "{{topic}}-{{partition}}"
        }
      ]
    }
  ]
}
```

---

## 5. Structured Logging

### Log4j2 config for Kafka Connect (`config/connect-log4j.properties`)

```properties
log4j.rootLogger=INFO, stdout
log4j.appender.stdout=org.apache.log4j.ConsoleAppender
log4j.appender.stdout.layout=org.apache.log4j.PatternLayout
log4j.appender.stdout.layout.ConversionPattern=%d{ISO8601} %-5p %c{1}:%L - %m%n

# More verbose for debugging connectors
log4j.logger.org.apache.kafka.connect.runtime=DEBUG
log4j.logger.io.debezium=DEBUG
```

### Python consumer structured logging

```python
import logging
import json

class JSONFormatter(logging.Formatter):
    def format(self, record):
        return json.dumps({
            "timestamp": self.formatTime(record),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "consumer_group": "my-group",
            "service": "pipeline-consumer",
        })

handler = logging.StreamHandler()
handler.setFormatter(JSONFormatter())
logging.getLogger().addHandler(handler)
```

---

## 6. Consumer Lag Monitoring Script

```bash
#!/bin/bash
# scripts/check-lag.sh

BOOTSTRAP="localhost:9092"
THRESHOLD=1000

echo "=== Consumer Group Lag Report ==="
echo "Timestamp: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo ""

docker exec kafka kafka-consumer-groups \
  --bootstrap-server $BOOTSTRAP \
  --list | while read group; do
    lag=$(docker exec kafka kafka-consumer-groups \
      --bootstrap-server $BOOTSTRAP \
      --group "$group" \
      --describe 2>/dev/null | \
      awk 'NR>1 && $6 ~ /^[0-9]+$/ {sum += $6} END {print sum+0}')

    if [ "$lag" -gt "$THRESHOLD" ]; then
      echo "⚠ HIGH LAG: Group=$group Lag=$lag"
    else
      echo "✓ OK: Group=$group Lag=$lag"
    fi
  done
```
