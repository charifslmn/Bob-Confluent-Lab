---
name: snowflake-connector
description: Connect Confluent Cloud Kafka topics to Snowflake via the managed Snowflake Sink Connector — configure Snowflake CLI (snow), set up RSA key-pair authentication, construct Snowpipe Streaming JSON configurations, enable automated schematization, and query live ingested data.
---

# Snowflake Sink Connector & Snowpipe Streaming Guide

A comprehensive architectural and operational guide for integrating Confluent Cloud with Snowflake using the managed **Snowflake Sink Connector** in **Snowpipe Streaming** mode.

---

## 1. Snowflake CLI Setup & Schema Provisioning

### Establish Snowflake CLI Connection
```bash
snow connection add \
  --connection-name <CONNECTION_NAME> \
  --account <ACCOUNT_IDENTIFIER> \
  --user <USERNAME> \
  --warehouse <WAREHOUSE_NAME> \
  --database SNOWFLAKE \
  --schema PUBLIC
```
* Note: Point default connection parameters to `--database SNOWFLAKE --schema PUBLIC` during initialization before custom databases exist.
* Verify session connectivity:
```bash
snow sql -q "SELECT CURRENT_USER(), CURRENT_ACCOUNT();" --connection <CONNECTION_NAME>
```

### Provision Target Database & Schema
```bash
snow sql --connection <CONNECTION_NAME> -q "
CREATE DATABASE IF NOT EXISTS <DATABASE_NAME>;
CREATE SCHEMA IF NOT EXISTS <DATABASE_NAME>.<SCHEMA_NAME>;
"
```

> ⚠️ **CRITICAL ARCHITECTURAL RULE — Automatic Table Provisioning:**
> Do **NOT** pre-create the target table with manual DDL. When running the Snowflake Sink connector in `SNOWPIPE_STREAMING` mode with `"snowflake.enable.schematization": "true"`, the connector automatically provisions the destination table and handles ongoing schema evolution based on the Kafka record schema. Pre-existing tables can cause schema reconciliation conflicts.

---

## 2. RSA Key Pair Authentication

The Confluent managed Snowflake Sink Connector uses **RSA key-pair authentication** for secure programmatic ingestion.

### Key Pair Generation (PKCS#8 Unencrypted)
```bash
# Generate 2048-bit RSA private key in PKCS8 format
openssl genrsa 2048 | openssl pkcs8 -topk8 -nocrypt -out /tmp/snowflake_key.p8

# Derive the corresponding public key
openssl rsa -in /tmp/snowflake_key.p8 -pubout -out /tmp/snowflake_key.pub
```

### Assign Public Key to Snowflake Identity
```bash
PUB_KEY=$(cat /tmp/snowflake_key.pub | grep -v "PUBLIC KEY" | tr -d '\n')
snow sql --connection <CONNECTION_NAME> -q "ALTER USER <USERNAME> SET RSA_PUBLIC_KEY='$PUB_KEY';"
```

---

## 3. Connector JSON Configuration Patterns

### Key Configuration Specifications
| Parameter | Setting | Enterprise Requirement |
|---|---|---|
| `snowflake.ingestion.method` | `SNOWPIPE_STREAMING` | High-throughput, sub-minute latency streaming ingestion. |
| `snowflake.enable.schematization` | `"true"` | **Must be lowercase boolean string `"true"`** (validates against schema registry/record fields to create named columns). |
| `snowflake.role.name` | `<ROLE_NAME>` | **Required** when using `SNOWPIPE_STREAMING`. |
| `snowflake.private.key` | `<BASE64_KEY>` | Raw base64 private key without PEM headers or newlines. |

### Configuration Generation (Python Script Template)
```python
import json

# Extract base64 private key safely across platforms
with open('/tmp/snowflake_key.p8') as f:
    lines = f.readlines()
private_key = ''.join(l.strip() for l in lines if 'PRIVATE KEY' not in l)

config = {
  "name": "<CONNECTOR_NAME>",
  "config": {
    "connector.class": "SnowflakeSink",
    "kafka.auth.mode": "KAFKA_API_KEY",
    "kafka.api.key": "<KAFKA_API_KEY>",
    "kafka.api.secret": "<KAFKA_API_SECRET>",
    "topics": "<TOPIC_NAME>",
    "input.data.format": "AVRO",  # or JSON / JSON_SR / PROTOBUF
    "snowflake.url.name": "<ACCOUNT_IDENTIFIER>.snowflakecomputing.com",
    "snowflake.user.name": "<USERNAME>",
    "snowflake.private.key": private_key,
    "snowflake.role.name": "<ROLE_NAME>",
    "snowflake.database.name": "<DATABASE_NAME>",
    "snowflake.schema.name": "<SCHEMA_NAME>",
    "snowflake.ingestion.method": "SNOWPIPE_STREAMING",
    "snowflake.enable.schematization": "true",
    "tasks.max": "1"
  }
}

with open('/tmp/connector-snowflake-sink.json', 'w') as f:
    json.dump(config, f, indent=2)
```

---

## 4. Deployment, Updates & Diagnostics

### Pre-Deployment Verification
Before deploying the connector, always verify that the source Kafka topic is actively provisioned:
```bash
confluent kafka topic list --cluster <CLUSTER_ID>
```

### Deploy Connector
```bash
confluent connect cluster create \
  --config-file /tmp/connector-snowflake-sink.json \
  --cluster <CLUSTER_ID>
```

### In-Place Configuration Update
To update connector configuration without teardown:
```bash
confluent connect cluster update <CONNECTOR_ID> \
  --config-file /tmp/connector-snowflake-sink.json \
  --cluster <CLUSTER_ID>
```

---

## 5. Ingestion Verification in Snowflake

> ⏳ **Ingestion Latency:** Snowpipe Streaming buffers micro-batches before committing. Allow ~1–2 minutes after connector reaches `RUNNING` status for initial table creation and data visibility.

### Check Record Ingestion Count
```bash
snow sql --connection <CONNECTION_NAME> -q "SELECT COUNT(*) FROM <DATABASE_NAME>.<SCHEMA_NAME>.<TABLE_NAME>;"
```

### Query Streamed Records
```bash
snow sql --connection <CONNECTION_NAME> -q "
SELECT *
FROM <DATABASE_NAME>.<SCHEMA_NAME>.<TABLE_NAME>
LIMIT 10;
"
```
