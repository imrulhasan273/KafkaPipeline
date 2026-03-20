# Schema Management

## Why Schema Management Matters

Without a schema registry:
- Producers can change formats silently → consumers crash
- No way to enforce backward/forward compatibility
- Schema duplication wastes bandwidth
- No central documentation of data contracts

---

## Schema Registry Concepts

### Subjects
Each topic has two subjects by default:
- `<topic>-key` — schema for the message key
- `<topic>-value` — schema for the message value

### Compatibility Levels

| Level | New Schema Can... | Old Consumer... |
|-------|------------------|-----------------|
| `BACKWARD` | Remove fields, add optional fields | Can read new data |
| `FORWARD` | Add fields, remove optional fields | Can read old data |
| `FULL` | Both BACKWARD + FORWARD | Works both ways |
| `BACKWARD_TRANSITIVE` | BACKWARD across all versions | — |
| `FORWARD_TRANSITIVE` | FORWARD across all versions | — |
| `FULL_TRANSITIVE` | Both across all versions | — |
| `NONE` | Anything goes | No guarantee |

**Production recommendation:** Use `BACKWARD` for most cases. Consumers can always handle new optional fields.

---

## Avro Schema Examples

### `schemas/order.avsc`

```json
{
  "type": "record",
  "name": "Order",
  "namespace": "com.pipeline.ecommerce",
  "doc": "Order record from CDC pipeline",
  "fields": [
    {
      "name": "id",
      "type": "long",
      "doc": "Primary key"
    },
    {
      "name": "customer_id",
      "type": "long"
    },
    {
      "name": "product_id",
      "type": "long"
    },
    {
      "name": "quantity",
      "type": "int"
    },
    {
      "name": "amount",
      "type": {
        "type": "bytes",
        "logicalType": "decimal",
        "precision": 10,
        "scale": 2
      }
    },
    {
      "name": "status",
      "type": {
        "type": "enum",
        "name": "OrderStatus",
        "symbols": ["PENDING", "PROCESSING", "COMPLETED", "CANCELLED", "REFUNDED"]
      }
    },
    {
      "name": "created_at",
      "type": {
        "type": "long",
        "logicalType": "timestamp-millis"
      }
    },
    {
      "name": "updated_at",
      "type": ["null", {
        "type": "long",
        "logicalType": "timestamp-millis"
      }],
      "default": null
    },
    {
      "name": "__op",
      "type": ["null", "string"],
      "default": null,
      "doc": "CDC operation: c=create, u=update, d=delete, r=read"
    },
    {
      "name": "__ts_ms",
      "type": ["null", "long"],
      "default": null,
      "doc": "CDC event timestamp in milliseconds"
    }
  ]
}
```

### `schemas/customer.avsc`

```json
{
  "type": "record",
  "name": "Customer",
  "namespace": "com.pipeline.ecommerce",
  "fields": [
    {"name": "id",         "type": "long"},
    {"name": "name",       "type": "string"},
    {"name": "email",      "type": "string"},
    {"name": "phone",      "type": ["null", "string"], "default": null},
    {"name": "created_at", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    {"name": "__op",       "type": ["null", "string"], "default": null},
    {"name": "__ts_ms",    "type": ["null", "long"],   "default": null}
  ]
}
```

---

## Schema Evolution Rules

### BACKWARD compatible changes (safe):
```
✓ Add a new optional field with a default value
  Before: {"name": "id", "type": "long"}
  After:  {"name": "id", "type": "long"}, {"name": "region", "type": ["null", "string"], "default": null}

✓ Remove a field (old consumers just ignore the missing data)
✓ Make a required field optional (add null union)
```

### BACKWARD incompatible changes (breaking):
```
✗ Add a required field without default → old consumers can't read old data
✗ Change field type (long → string)
✗ Remove a field that consumers depend on
✗ Rename a field
```

### How to Handle Renaming (without breaking):
```json
// Version 1
{"name": "user_name", "type": "string"}

// Version 2 — add alias
{"name": "customer_name", "type": "string", "aliases": ["user_name"]}

// Version 3 — consumers updated, remove alias
{"name": "customer_name", "type": "string"}
```

---

## Schema Registry REST API

> **Platform note:** On Windows PowerShell, use `curl.exe` instead of `curl`. In PowerShell, `curl` is an alias for `Invoke-WebRequest`. On Linux (AlmaLinux 9) and macOS, `curl` works as-is.

**Linux (AlmaLinux 9) / macOS (M1/M2/M3):**

```bash
# List all subjects
curl http://localhost:8081/subjects

# Get all versions of a subject
curl http://localhost:8081/subjects/prod.mysql.sourcedb.orders-value/versions

# Get a specific version
curl http://localhost:8081/subjects/prod.mysql.sourcedb.orders-value/versions/1

# Get the latest version
curl http://localhost:8081/subjects/prod.mysql.sourcedb.orders-value/versions/latest

# Register a new schema manually
curl -X POST http://localhost:8081/subjects/prod.mysql.sourcedb.orders-value/versions \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d "{\"schema\": $(cat schemas/order.avsc | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read()))')}"

# Check compatibility before registering
curl -X POST http://localhost:8081/compatibility/subjects/prod.mysql.sourcedb.orders-value/versions/latest \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d "{\"schema\": $(cat schemas/order_v2.avsc | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read()))')}"

# Set compatibility level for a subject
curl -X PUT http://localhost:8081/config/prod.mysql.sourcedb.orders-value \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"compatibility": "BACKWARD"}'

# Set global compatibility level
curl -X PUT http://localhost:8081/config \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"compatibility": "BACKWARD"}'

# Delete a schema version (soft delete)
curl -X DELETE http://localhost:8081/subjects/prod.mysql.sourcedb.orders-value/versions/1

# Delete all versions (soft delete)
curl -X DELETE http://localhost:8081/subjects/prod.mysql.sourcedb.orders-value
```

**Windows PowerShell:**

```powershell
# List all subjects
curl.exe http://localhost:8081/subjects

# Get all versions of a subject
curl.exe http://localhost:8081/subjects/prod.mysql.sourcedb.orders-value/versions

# Get a specific version
curl.exe http://localhost:8081/subjects/prod.mysql.sourcedb.orders-value/versions/1

# Get the latest version
curl.exe http://localhost:8081/subjects/prod.mysql.sourcedb.orders-value/versions/latest

# Set compatibility level for a subject
curl.exe -X PUT http://localhost:8081/config/prod.mysql.sourcedb.orders-value `
  -H "Content-Type: application/vnd.schemaregistry.v1+json" `
  -d '{"compatibility": "BACKWARD"}'

# Set global compatibility level
curl.exe -X PUT http://localhost:8081/config `
  -H "Content-Type: application/vnd.schemaregistry.v1+json" `
  -d '{"compatibility": "BACKWARD"}'

# Delete a schema version (soft delete)
curl.exe -X DELETE http://localhost:8081/subjects/prod.mysql.sourcedb.orders-value/versions/1

# Delete all versions (soft delete)
curl.exe -X DELETE http://localhost:8081/subjects/prod.mysql.sourcedb.orders-value
```

---

## Protobuf Schema Example

```protobuf
// schemas/order.proto
syntax = "proto3";
package com.pipeline.ecommerce;

import "google/protobuf/timestamp.proto";

message Order {
  int64 id          = 1;
  int64 customer_id = 2;
  int64 product_id  = 3;
  int32 quantity    = 4;
  double amount     = 5;
  string status     = 6;
  google.protobuf.Timestamp created_at = 7;
  google.protobuf.Timestamp updated_at = 8;
  string cdc_op     = 9;
  int64 cdc_ts_ms   = 10;
}
```

### Kafka Connect config for Protobuf:
```json
{
  "key.converter": "io.confluent.connect.protobuf.ProtobufConverter",
  "key.converter.schema.registry.url": "http://schema-registry:8081",
  "value.converter": "io.confluent.connect.protobuf.ProtobufConverter",
  "value.converter.schema.registry.url": "http://schema-registry:8081"
}
```

---

## JSON Schema Example

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "$id": "http://pipeline.com/schemas/order.json",
  "title": "Order",
  "type": "object",
  "required": ["id", "customer_id", "amount", "status"],
  "properties": {
    "id":          {"type": "integer"},
    "customer_id": {"type": "integer"},
    "product_id":  {"type": "integer"},
    "quantity":    {"type": "integer", "minimum": 1},
    "amount":      {"type": "number", "minimum": 0},
    "status":      {"type": "string", "enum": ["PENDING","PROCESSING","COMPLETED","CANCELLED"]},
    "created_at":  {"type": "string", "format": "date-time"},
    "__op":        {"type": "string", "enum": ["c","u","d","r"]},
    "__ts_ms":     {"type": "integer"}
  },
  "additionalProperties": true
}
```

---

## Schema Migration Workflow

```
1. Developer proposes schema change
         ↓
2. Run compatibility check against registry
   curl -X POST /compatibility/subjects/...
         ↓
3. If compatible → register new schema version
         ↓
4. Deploy updated producer (writes new schema version)
         ↓
5. Old consumers continue reading (BACKWARD compat)
         ↓
6. Update consumers at leisure
         ↓
7. Remove deprecated fields in next cycle
```

---

## Schema Registry in Production

```yaml
# High availability: 2+ instances behind load balancer
schema-registry-1:
  image: confluentinc/cp-schema-registry:7.6.1
  environment:
    SCHEMA_REGISTRY_HOST_NAME: schema-registry-1
    SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: "kafka-broker-1:9092,kafka-broker-2:9092,kafka-broker-3:9092"
    SCHEMA_REGISTRY_SCHEMA_COMPATIBILITY_LEVEL: "BACKWARD"
    SCHEMA_REGISTRY_MASTER_ELIGIBILITY: "true"
    SCHEMA_REGISTRY_HEAP_OPTS: "-Xms512m -Xmx1g"

schema-registry-2:
  image: confluentinc/cp-schema-registry:7.6.1
  environment:
    SCHEMA_REGISTRY_HOST_NAME: schema-registry-2
    SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: "kafka-broker-1:9092,kafka-broker-2:9092,kafka-broker-3:9092"
    SCHEMA_REGISTRY_SCHEMA_COMPATIBILITY_LEVEL: "BACKWARD"
    SCHEMA_REGISTRY_MASTER_ELIGIBILITY: "true"
```
