# Security

## Security Layers

```
┌──────────────────────────────────────────────────┐
│ Layer 4: Secrets Management                      │
│ HashiCorp Vault / AWS Secrets Manager / K8s Sec  │
├──────────────────────────────────────────────────┤
│ Layer 3: Authorization (ACLs)                    │
│ Who can read/write which topics                  │
├──────────────────────────────────────────────────┤
│ Layer 2: Authentication (SASL)                   │
│ PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, GSSAPI      │
├──────────────────────────────────────────────────┤
│ Layer 1: Encryption in Transit (TLS/SSL)         │
│ Client ↔ Broker, Broker ↔ Broker                 │
└──────────────────────────────────────────────────┘
```

---

## 1. TLS/SSL Encryption

### Generate Certificates (Development)

```bash
#!/bin/bash
# scripts/generate-certs.sh

VALIDITY_DAYS=365
PASSWORD="changeme123"
CERT_DIR="./certs"
mkdir -p $CERT_DIR

# Generate CA key and certificate
openssl req -new -x509 -keyout $CERT_DIR/ca-key.pem \
  -out $CERT_DIR/ca-cert.pem \
  -days $VALIDITY_DAYS \
  -passout pass:$PASSWORD \
  -subj "/CN=KafkaCA/OU=Pipeline/O=Company/L=NYC/S=NY/C=US"

echo "CA certificate generated."

# Generate broker keystore
keytool -keystore $CERT_DIR/kafka.server.keystore.jks \
  -alias localhost \
  -validity $VALIDITY_DAYS \
  -genkey \
  -keyalg RSA \
  -keysize 2048 \
  -dname "CN=kafka,OU=Pipeline,O=Company,L=NYC,S=NY,C=US" \
  -storepass $PASSWORD \
  -keypass $PASSWORD

# Generate certificate signing request
keytool -keystore $CERT_DIR/kafka.server.keystore.jks \
  -alias localhost \
  -certreq \
  -file $CERT_DIR/cert-file.csr \
  -storepass $PASSWORD

# Sign CSR with CA
openssl x509 -req \
  -CA $CERT_DIR/ca-cert.pem \
  -CAkey $CERT_DIR/ca-key.pem \
  -in $CERT_DIR/cert-file.csr \
  -out $CERT_DIR/cert-signed.pem \
  -days $VALIDITY_DAYS \
  -CAcreateserial \
  -passin pass:$PASSWORD

# Import CA cert into keystore
keytool -keystore $CERT_DIR/kafka.server.keystore.jks \
  -alias CARoot \
  -import \
  -file $CERT_DIR/ca-cert.pem \
  -storepass $PASSWORD \
  -noprompt

# Import signed cert into keystore
keytool -keystore $CERT_DIR/kafka.server.keystore.jks \
  -alias localhost \
  -import \
  -file $CERT_DIR/cert-signed.pem \
  -storepass $PASSWORD \
  -noprompt

# Create truststore with CA cert
keytool -keystore $CERT_DIR/kafka.server.truststore.jks \
  -alias CARoot \
  -import \
  -file $CERT_DIR/ca-cert.pem \
  -storepass $PASSWORD \
  -noprompt

echo "Certificates generated in $CERT_DIR/"
```

### Kafka Broker TLS Config

```properties
# server.properties
listeners=PLAINTEXT://0.0.0.0:9092,SSL://0.0.0.0:9093
advertised.listeners=PLAINTEXT://kafka:9092,SSL://kafka:9093
inter.broker.listener.name=SSL

ssl.keystore.location=/etc/kafka/secrets/kafka.server.keystore.jks
ssl.keystore.password=changeme123
ssl.key.password=changeme123
ssl.truststore.location=/etc/kafka/secrets/kafka.server.truststore.jks
ssl.truststore.password=changeme123
ssl.endpoint.identification.algorithm=
ssl.client.auth=required
```

### Client TLS Config (producer/consumer)

```properties
security.protocol=SSL
ssl.truststore.location=/path/to/client.truststore.jks
ssl.truststore.password=changeme123
ssl.keystore.location=/path/to/client.keystore.jks
ssl.keystore.password=changeme123
ssl.key.password=changeme123
```

---

## 2. SASL Authentication

### SASL/SCRAM-SHA-512 (Recommended for production)

#### Step 1: Create users in Kafka

```bash
# Create admin user
docker exec kafka kafka-configs \
  --bootstrap-server localhost:9092 \
  --alter \
  --add-config 'SCRAM-SHA-512=[iterations=8192,password=admin-password]' \
  --entity-type users \
  --entity-name admin

# Create producer user
docker exec kafka kafka-configs \
  --bootstrap-server localhost:9092 \
  --alter \
  --add-config 'SCRAM-SHA-512=[iterations=8192,password=producer-password]' \
  --entity-type users \
  --entity-name pipeline-producer

# Create consumer user
docker exec kafka kafka-configs \
  --bootstrap-server localhost:9092 \
  --alter \
  --add-config 'SCRAM-SHA-512=[iterations=8192,password=consumer-password]' \
  --entity-type users \
  --entity-name pipeline-consumer

# Create connect user
docker exec kafka kafka-configs \
  --bootstrap-server localhost:9092 \
  --alter \
  --add-config 'SCRAM-SHA-512=[iterations=8192,password=connect-password]' \
  --entity-type users \
  --entity-name kafka-connect
```

#### Step 2: Broker SASL config

```properties
# server.properties
listeners=SASL_SSL://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
advertised.listeners=SASL_SSL://kafka:9092
listener.security.protocol.map=SASL_SSL:SASL_SSL,CONTROLLER:PLAINTEXT
inter.broker.listener.name=SASL_SSL

sasl.enabled.mechanisms=SCRAM-SHA-512
sasl.mechanism.inter.broker.protocol=SCRAM-SHA-512

# JAAS config
listener.name.sasl_ssl.scram-sha-512.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
  username="admin" \
  password="admin-password";

# SSL settings (same as above)
ssl.keystore.location=/etc/kafka/secrets/kafka.server.keystore.jks
ssl.keystore.password=changeme123
ssl.truststore.location=/etc/kafka/secrets/kafka.server.truststore.jks
ssl.truststore.password=changeme123
```

#### Step 3: Client SASL config

```properties
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
  username="pipeline-producer" \
  password="producer-password";

ssl.truststore.location=/path/to/client.truststore.jks
ssl.truststore.password=changeme123
```

#### Kafka Connect SASL Config

```json
{
  "security.protocol": "SASL_SSL",
  "sasl.mechanism": "SCRAM-SHA-512",
  "sasl.jaas.config": "org.apache.kafka.common.security.scram.ScramLoginModule required username='kafka-connect' password='connect-password';",
  "ssl.truststore.location": "/etc/kafka/secrets/client.truststore.jks",
  "ssl.truststore.password": "changeme123",
  "producer.security.protocol": "SASL_SSL",
  "producer.sasl.mechanism": "SCRAM-SHA-512",
  "producer.sasl.jaas.config": "...",
  "consumer.security.protocol": "SASL_SSL",
  "consumer.sasl.mechanism": "SCRAM-SHA-512",
  "consumer.sasl.jaas.config": "..."
}
```

---

## 3. Authorization (ACLs)

### Grant topic-level ACLs

```bash
KAFKA="docker exec kafka kafka-acls --bootstrap-server localhost:9092"

# Producer: allow pipeline-producer to write to CDC topics
$KAFKA --add \
  --allow-principal User:pipeline-producer \
  --operation Write \
  --operation Describe \
  --topic 'prod.mysql.*' \
  --resource-pattern-type prefixed

# Consumer: allow pipeline-consumer to read
$KAFKA --add \
  --allow-principal User:pipeline-consumer \
  --operation Read \
  --operation Describe \
  --topic 'prod.mysql.*' \
  --resource-pattern-type prefixed

# Consumer group ACL
$KAFKA --add \
  --allow-principal User:pipeline-consumer \
  --operation Read \
  --group 'connect-*' \
  --resource-pattern-type prefixed

# Kafka Connect: full access to internal topics
$KAFKA --add \
  --allow-principal User:kafka-connect \
  --operation All \
  --topic '_connect-*' \
  --resource-pattern-type prefixed

# DLQ write access
$KAFKA --add \
  --allow-principal User:kafka-connect \
  --operation Write \
  --topic 'prod.dlq.*' \
  --resource-pattern-type prefixed

# List all ACLs
$KAFKA --list

# List ACLs for a specific user
$KAFKA --list --principal User:pipeline-producer
```

### ACL Matrix (Reference)

| Principal | Resource | Operation |
|-----------|----------|-----------|
| `pipeline-producer` | `prod.mysql.*` topics | Write, Describe |
| `pipeline-consumer` | `prod.mysql.*` topics | Read, Describe |
| `pipeline-consumer` | `connect-*` groups | Read |
| `kafka-connect` | `_connect-*` topics | All |
| `kafka-connect` | `prod.*` topics | Read, Write |
| `kafka-connect` | `prod.dlq.*` topics | Write |
| `admin` | All | All |

---

## 4. Secrets Management

### Using Docker Compose secrets (for dev)

```yaml
# docker-compose.yml
services:
  kafka-connect:
    secrets:
      - db_password
    environment:
      DB_PASSWORD_FILE: /run/secrets/db_password

secrets:
  db_password:
    file: ./secrets/db_password.txt
```

### Using Kafka Connect ExternalSecret (FileConfigProvider)

```json
{
  "connection.password": "${file:/etc/kafka-connect/secrets/db.properties:password}"
}
```

```properties
# /etc/kafka-connect/secrets/db.properties
password=my_secret_password
```

### Using HashiCorp Vault with Kafka Connect

```json
{
  "config.providers": "vault",
  "config.providers.vault.class": "io.confluent.connect.vault.VaultConfigProvider",
  "config.providers.vault.param.vault.addr": "http://vault:8200",
  "config.providers.vault.param.vault.token": "root",

  "connection.password": "${vault:secret/data/kafka-connect/db#password}"
}
```

### Kubernetes Secrets (production)

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: kafka-connect-db-secret
  namespace: kafka
type: Opaque
stringData:
  db-password: "my_secure_password"
  db-user: "kafka_user"
---
# Reference in deployment
env:
  - name: DB_PASSWORD
    valueFrom:
      secretKeyRef:
        name: kafka-connect-db-secret
        key: db-password
```

---

## 5. Network Security

```yaml
# docker-compose.yml — isolate Kafka network
networks:
  kafka-internal:
    driver: bridge
    internal: true    # No external internet access
  kafka-external:
    driver: bridge

services:
  kafka:
    networks:
      - kafka-internal
  kafka-connect:
    networks:
      - kafka-internal
      - kafka-external   # Connect needs access to source/target DBs
  kafka-ui:
    networks:
      - kafka-internal
    ports:
      - "127.0.0.1:8080:8080"   # Bind to localhost only
```

---

## 6. Security Checklist

```
Authentication
  ✓ SASL/SCRAM-SHA-512 enabled on all clients
  ✓ SASL/GSSAPI (Kerberos) for enterprise environments
  ✓ mTLS for broker-to-broker communication
  ✓ Default passwords changed

Encryption
  ✓ TLS 1.2+ for all connections
  ✓ Certificates from trusted CA
  ✓ Certificate rotation process documented
  ✓ Secrets not in environment variables or config files in plaintext

Authorization
  ✓ ACLs defined for every user/service account
  ✓ Principle of least privilege applied
  ✓ Admin access restricted to ops team
  ✓ ACL audit logs enabled

Network
  ✓ Kafka not exposed to internet
  ✓ Firewall rules: only authorized services can reach port 9092
  ✓ Kafka UI/Connect API not publicly accessible
  ✓ VPC/private network isolation

Operational
  ✓ Audit logging enabled (kafka.authorizer.logger)
  ✓ Log rotation configured
  ✓ Security incidents runbook documented
  ✓ Regular certificate rotation schedule
```
