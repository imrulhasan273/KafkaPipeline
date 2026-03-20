# Deployment

## Docker Compose (Development & Staging)

See [03-environment-setup.md](03-environment-setup.md) for the full `docker-compose.yml`.

> **Why `docker compose` (not `docker-compose`):** The `docker-compose` command (with a hyphen) is the old standalone CLI v1. Docker 29.x ships `docker compose` (subcommand, no hyphen) as the v2 plugin. Use `docker compose` for all new projects.

### Quick start commands

All `docker compose` commands work identically on Windows PowerShell, Linux (AlmaLinux 9), and macOS (M1/M2/M3):

```bash
# Start all services
docker compose up -d

# Start specific services
docker compose up -d kafka schema-registry kafka-connect

# View logs
docker compose logs -f kafka-connect

# Scale Connect workers
docker compose up -d --scale kafka-connect=3

# Stop and clean up
docker compose down -v   # -v removes volumes (data loss!)
docker compose down      # Keep volumes
```

**Windows PowerShell — additional tips:**

```powershell
# Open Kafka UI in browser
Start-Process "http://localhost:8090"

# Check service health
docker compose ps

# Restart a specific service
docker compose restart kafka-connect
```

**Linux (AlmaLinux 9) / macOS (M1/M2/M3):**

```bash
# Watch service status
watch -n 2 docker compose ps

# Follow logs for multiple services
docker compose logs -f kafka kafka-connect
```

---

## Kubernetes Deployment

### Helm Charts (Recommended)

```bash
# Add Confluent Helm repo
helm repo add confluentinc https://packages.confluent.io/helm
helm repo update

# Install Confluent Platform (all-in-one)
helm install confluent-operator confluentinc/confluent-for-kubernetes \
  --namespace kafka \
  --create-namespace

# Install Kafka cluster via CRD
kubectl apply -f k8s/kafka-cluster.yaml
```

### `k8s/namespace.yaml`

```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: kafka
  labels:
    app.kubernetes.io/name: kafka
    environment: production
```

### `k8s/kafka-cluster.yaml` (Strimzi CRD)

```yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: Kafka
metadata:
  name: kafka-cluster
  namespace: kafka
spec:
  kafka:
    version: 3.7.0
    replicas: 3
    listeners:
      - name: plain
        port: 9092
        type: internal
        tls: false
      - name: tls
        port: 9093
        type: internal
        tls: true
      - name: external
        port: 9094
        type: loadbalancer
        tls: true
    config:
      offsets.topic.replication.factor: 3
      transaction.state.log.replication.factor: 3
      transaction.state.log.min.isr: 2
      default.replication.factor: 3
      min.insync.replicas: 2
      auto.create.topics.enable: "false"
      log.retention.hours: 168
      compression.type: snappy
    storage:
      type: jbod
      volumes:
        - id: 0
          type: persistent-claim
          size: 500Gi
          class: fast-ssd
    resources:
      requests:
        memory: 8Gi
        cpu: "2"
      limits:
        memory: 16Gi
        cpu: "4"
    jvmOptions:
      -Xms: 4096m
      -Xmx: 4096m
    metricsConfig:
      type: jmxPrometheusExporter
      valueFrom:
        configMapKeyRef:
          name: kafka-metrics
          key: kafka-metrics-config.yml

  zookeeper:
    replicas: 3
    storage:
      type: persistent-claim
      size: 10Gi
      class: fast-ssd
    resources:
      requests:
        memory: 1Gi
        cpu: "0.5"
      limits:
        memory: 2Gi
        cpu: "1"

  entityOperator:
    topicOperator: {}
    userOperator: {}
```

### `k8s/kafka-connect-deployment.yaml`

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kafka-connect
  namespace: kafka
  labels:
    app: kafka-connect
spec:
  replicas: 3
  selector:
    matchLabels:
      app: kafka-connect
  template:
    metadata:
      labels:
        app: kafka-connect
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "7072"
    spec:
      containers:
        - name: kafka-connect
          image: debezium/connect:2.6
          ports:
            - containerPort: 8083
              name: rest-api
            - containerPort: 7072
              name: metrics
          env:
            - name: BOOTSTRAP_SERVERS
              value: "kafka-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092"
            - name: GROUP_ID
              value: "kafka-connect-cluster"
            - name: CONFIG_STORAGE_TOPIC
              value: "_connect-configs"
            - name: OFFSET_STORAGE_TOPIC
              value: "_connect-offsets"
            - name: STATUS_STORAGE_TOPIC
              value: "_connect-status"
            - name: KEY_CONVERTER
              value: "org.apache.kafka.connect.json.JsonConverter"
            - name: VALUE_CONVERTER
              value: "org.apache.kafka.connect.json.JsonConverter"
            - name: KEY_CONVERTER_SCHEMA_REGISTRY_URL
              value: "http://schema-registry.kafka.svc.cluster.local:8081"
            - name: VALUE_CONVERTER_SCHEMA_REGISTRY_URL
              value: "http://schema-registry.kafka.svc.cluster.local:8081"
            - name: DB_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: kafka-connect-secrets
                  key: db-password
            - name: KAFKA_HEAP_OPTS
              value: "-Xms2g -Xmx4g"
          resources:
            requests:
              memory: 2Gi
              cpu: "1"
            limits:
              memory: 4Gi
              cpu: "2"
          readinessProbe:
            httpGet:
              path: /
              port: 8083
            initialDelaySeconds: 30
            periodSeconds: 10
          livenessProbe:
            httpGet:
              path: /
              port: 8083
            initialDelaySeconds: 60
            periodSeconds: 30
          volumeMounts:
            - name: connector-config
              mountPath: /kafka/connect/config
      volumes:
        - name: connector-config
          configMap:
            name: kafka-connect-config
---
apiVersion: v1
kind: Service
metadata:
  name: kafka-connect
  namespace: kafka
spec:
  selector:
    app: kafka-connect
  ports:
    - port: 8083
      targetPort: 8083
      name: rest-api
  type: ClusterIP
```

### `k8s/schema-registry.yaml`

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: schema-registry
  namespace: kafka
spec:
  replicas: 2
  selector:
    matchLabels:
      app: schema-registry
  template:
    metadata:
      labels:
        app: schema-registry
    spec:
      containers:
        - name: schema-registry
          image: confluentinc/cp-schema-registry:7.6.1
          ports:
            - containerPort: 8081
          env:
            - name: SCHEMA_REGISTRY_HOST_NAME
              valueFrom:
                fieldRef:
                  fieldPath: status.podIP
            - name: SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS
              value: "kafka-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092"
            - name: SCHEMA_REGISTRY_SCHEMA_COMPATIBILITY_LEVEL
              value: "BACKWARD"
          resources:
            requests:
              memory: 512Mi
              cpu: "0.5"
            limits:
              memory: 1Gi
              cpu: "1"
---
apiVersion: v1
kind: Service
metadata:
  name: schema-registry
  namespace: kafka
spec:
  selector:
    app: schema-registry
  ports:
    - port: 8081
      targetPort: 8081
  type: ClusterIP
```

---

## CI/CD Pipeline

### GitHub Actions: `.github/workflows/deploy-connectors.yml`

```yaml
name: Deploy Kafka Connectors

on:
  push:
    branches: [main]
    paths:
      - 'connectors/**'

jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Validate connector configs
        run: |
          for f in connectors/**/*.json; do
            echo "Validating $f..."
            python3 -c "
          import json, sys
          with open('$f') as fp:
              cfg = json.load(fp)
          assert 'name' in cfg, 'Missing name'
          assert 'config' in cfg, 'Missing config'
          assert 'connector.class' in cfg['config'], 'Missing connector.class'
          print(f'  OK: {cfg[\"name\"]}')
            "
          done

      - name: Run connector tests
        run: |
          pip install pytest requests
          pytest tests/connectors/ -v

  deploy-staging:
    needs: validate
    runs-on: ubuntu-latest
    environment: staging
    steps:
      - uses: actions/checkout@v4

      - name: Deploy to Staging Connect
        env:
          CONNECT_URL: ${{ secrets.STAGING_CONNECT_URL }}
        run: |
          bash scripts/deploy-connectors.sh $CONNECT_URL connectors/

  deploy-production:
    needs: deploy-staging
    runs-on: ubuntu-latest
    environment: production
    steps:
      - uses: actions/checkout@v4

      - name: Deploy to Production Connect
        env:
          CONNECT_URL: ${{ secrets.PROD_CONNECT_URL }}
        run: |
          bash scripts/deploy-connectors.sh $CONNECT_URL connectors/
```

### `scripts/deploy-connectors.sh`

> **Platform note:** This is a bash script that runs on Linux agents and macOS. CI/CD pipelines (GitHub Actions `ubuntu-latest`) run on Linux, so this script works as-is. For local development on Windows, use Git Bash or WSL to run bash scripts.

```bash
#!/bin/bash
set -e

CONNECT_URL=$1
CONNECTOR_DIR=$2

deploy_connector() {
  local file=$1
  local name=$(python3 -c "import json; print(json.load(open('$file'))['name'])")

  echo "Deploying connector: $name from $file"

  # Check if connector exists
  status=$(curl -s -o /dev/null -w "%{http_code}" "$CONNECT_URL/connectors/$name")

  if [ "$status" == "200" ]; then
    echo "  Updating existing connector..."
    curl -s -X PUT "$CONNECT_URL/connectors/$name/config" \
      -H "Content-Type: application/json" \
      -d "$(python3 -c "import json; print(json.dumps(json.load(open('$file'))['config']))")"
  else
    echo "  Creating new connector..."
    curl -s -X POST "$CONNECT_URL/connectors" \
      -H "Content-Type: application/json" \
      -d @"$file"
  fi

  # Wait for RUNNING state
  for i in $(seq 1 30); do
    state=$(curl -s "$CONNECT_URL/connectors/$name/status" | \
            python3 -c "import json,sys; d=json.load(sys.stdin); print(d['connector']['state'])")
    if [ "$state" == "RUNNING" ]; then
      echo "  ✓ $name is RUNNING"
      return 0
    fi
    echo "  Waiting... ($state)"
    sleep 5
  done

  echo "  ✗ $name failed to reach RUNNING state"
  curl -s "$CONNECT_URL/connectors/$name/status" | python3 -m json.tool
  exit 1
}

for f in $CONNECTOR_DIR/**/*.json; do
  deploy_connector "$f"
done

echo "All connectors deployed successfully."
```

---

## Kubernetes Rolling Upgrade (Zero Downtime)

```bash
# Rolling restart of Kafka Connect pods
kubectl rollout restart deployment/kafka-connect -n kafka

# Watch rollout progress
kubectl rollout status deployment/kafka-connect -n kafka

# Rollback if needed
kubectl rollout undo deployment/kafka-connect -n kafka

# Kafka broker rolling upgrade (Strimzi)
kubectl annotate kafka kafka-cluster \
  strimzi.io/manual-rolling-update=true \
  -n kafka
```

---

## Resource Limits Reference

```yaml
# Development
kafka-broker:   2 vCPU, 4 GB RAM, 50 GB SSD
kafka-connect:  1 vCPU, 2 GB RAM
schema-registry: 0.5 vCPU, 512 MB RAM

# Production
kafka-broker:   4–8 vCPU, 16–32 GB RAM, 500 GB–2 TB NVMe SSD
kafka-connect:  2–4 vCPU, 4–8 GB RAM (per worker)
schema-registry: 1 vCPU, 1–2 GB RAM (per instance)
```
