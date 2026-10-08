# Snowflake Sink Connector — Test Reference, Working Config & Documentation

**Status: ✅ FULLY WORKING — JSON verified 2026-10-06 | AVRO verified 2026-10-07**

This document covers everything needed to stream data from a Confluent Cloud Kafka topic into a Snowflake table using the managed Snowflake Sink Connector. It includes the complete working configuration, all official documentation extracted, all errors encountered and how they were resolved.

---

## ✅ Test 1: JSON Format (Verified 2026-10-06)

- **Cluster type:** Basic (`$0.135/CKU-hour`)
- **Input format:** JSON (schemaless — no Schema Registry needed)
- **Datagen quickstart:** ORDERS
- **Ingestion method:** SNOWPIPE_STREAMING
- **Schematization:** enabled (`"true"` lowercase)
- **Auth:** RSA key pair on ACCOUNTADMIN user (CSAL)
- **Table creation:** fully automatic — connector created `TEST_STREAM` table with proper named columns
- **Data verified:** 449 rows on first check, 577 rows 30 seconds later → **live streaming confirmed**

### Resulting Snowflake Table Schema (auto-created by connector)
```
LAB.TEST.TEST_STREAM
├── RECORD_METADATA  VARIANT    (Kafka metadata: offset, partition, topic, timestamp)
├── ITEMID           TEXT
├── ORDERID          NUMBER
├── ORDERUNITS       FLOAT
├── ADDRESS          VARIANT    (nested JSON object)
└── ORDERTIME        NUMBER
```

---

## ✅ Test 2: AVRO Format — Full Flink Pipeline (Verified 2026-10-07, confirmed with live poll)

- **Cluster type:** Standard (`$0.75/CKU-hour`)
- **Input format:** AVRO (Schema Registry required — auto-provided by Confluent environment `ESSENTIALS` package)
- **Source:** Full Flink materialized table pipeline: `sample_data_users` + `sample_data_stock_trades` → `users_keyed` → `trades_enriched` → `trades_forecast`
- **Ingestion method:** SNOWPIPE_STREAMING
- **Schematization:** enabled (`"true"` lowercase)
- **Auth:** RSA key pair on ACCOUNTADMIN user (CSAL) — same key from Test 1, still valid
- **Snowflake target:** `LAB.TEST_AVRO.TRADES_FORECAST` (schema pre-created manually, table auto-created by connector)
- **Data verified:** 94 rows on first poll, grew to 101 → 108 rows over subsequent 30-second intervals → **live continuous Avro streaming confirmed** ✅

### Live Poll Results (30-second intervals, confirmed 2026-10-07)
```
Time         Row Count    Growth
------------------------------------
08:48:52            94      +94
08:49:27            94       --
08:50:02           100       +6
08:50:38           101       +1
08:51:12           101       --
08:51:47           101       --
  ... (connector still running)
  final check:     108       +7
```

> The row count grows slowly because ML_FORECAST needs a minimum of 10 tumbling windows of training data per symbol before it emits a forecast row. Once the training window is met per symbol, new rows arrive every 10 seconds per symbol. Growth rate depends on how many symbols have accumulated enough windows.

### Resulting Snowflake Table Schema (auto-created by connector with schematization)
```
LAB.TEST_AVRO.TRADES_FORECAST
├── RECORD_METADATA   VARIANT    (Kafka metadata: offset, partition, topic, timestamp)
├── SYMBOL            TEXT       (stock ticker symbol)
├── TS                NUMBER     (window end timestamp milliseconds)
├── CURRENT_COUNT     NUMBER     (actual trades in the latest 10s window)
├── FORECAST_COUNT    FLOAT      (ML_FORECAST predicted next-window volume)
└── UPPER_BOUND       FLOAT      (ML_FORECAST upper confidence interval)
```

### Sample Data (live rows from `SELECT ... ORDER BY TS DESC LIMIT 10`)
```
SYMBOL | TS            | CURRENT_COUNT | FORECAST_COUNT | UPPER_BOUND
ZWZZT  | 1791377350000 | 19            | 23.0           | 67.0
ZBZX   | 1791377350000 | 23            | 33.0           | 67.0
ZVZZT  | 1791377350000 | 21            | 32.0           | 74.0
ZJZZT  | 1791377350000 | 14            | 16.0           | 58.0
ZTEST  | 1791377350000 | 16            | 22.0           | 57.0
ZVV    | 1791377350000 | 13            | 16.0           | 57.0
ZXZZT  | 1791377350000 | 16            | 17.0           | 46.0
ZTEST  | 1791377280000 | 20            | 26.0           | 61.0
ZJZZT  | 1791377280000 | 15            | 16.0           | 60.0
ZVV    | 1791377280000 | 22            | 32.0           | 73.0
```

### The Exact Working AVRO Config
```json
{
  "connector.class": "SnowflakeSink",
  "name": "SnowflakeSink_test_avro",
  "kafka.auth.mode": "KAFKA_API_KEY",
  "topics": "trades_forecast",
  "input.data.format": "AVRO",
  "snowflake.url.name": "vrlnecq-xo34979.snowflakecomputing.com",
  "snowflake.user.name": "CSAL",
  "snowflake.private.key": "<BASE64_PRIVATE_KEY_NO_HEADERS>",
  "snowflake.role.name": "ACCOUNTADMIN",
  "snowflake.database.name": "LAB",
  "snowflake.schema.name": "TEST_AVRO",
  "snowflake.ingestion.method": "SNOWPIPE_STREAMING",
  "snowflake.enable.schematization": "true",
  "tasks.max": "1"
}
```

### How It Was Built — Step by Step (all Confluent CLI)
1. `confluent kafka cluster create test-avro-cluster --cloud aws --region us-east-2 --type standard`
2. `confluent api-key create --resource <CLUSTER_ID>` → `confluent api-key use <KEY>`
3. Deploy Users Datagen: `confluent connect cluster create --config-file connector-users-avro.json`
4. Deploy Stock Trades Datagen: `confluent connect cluster create --config-file connector-stock-trades-avro.json`
5. `confluent flink compute-pool create test-avro-pool --cloud aws --region us-east-2 --max-cfu 10 --wait`
6. Submit `users_keyed` materialized table (Flink statement, no `--wait`)
7. Submit `trades_enriched` materialized table (after `users_keyed` COMPLETED)
8. Submit `trades_forecast` materialized table (after `trades_enriched` COMPLETED)
9. Confirm CFU=3 and `trades_forecast` topic exists: `confluent kafka topic list`
10. Build Avro Snowflake Sink config via Python (private key embedded), deploy: `confluent connect cluster create`
11. Wait ~90 seconds → connector RUNNING, task RUNNING
12. Poll `SELECT COUNT(*) FROM LAB.TEST_AVRO.TRADES_FORECAST` every 30s → rows growing ✅
13. Teardown: delete connectors → suspend Flink tables (CFU→0) → delete pool → delete cluster

### Key Differences from JSON Test
| | JSON Test | AVRO Test |
|---|---|---|
| `input.data.format` | `JSON` | `AVRO` |
| Schema Registry needed? | No | Yes (auto-provided by Confluent env) |
| Source | ORDERS Datagen | Full Flink pipeline (`trades_forecast`) |
| Snowflake schema | `TEST` | `TEST_AVRO` |
| Cluster type | Basic | Standard |
| Named columns | Yes (schematization) | Yes (schematization) |
| Live streaming confirmed | ✅ 449 → 577 rows | ✅ 94 → 101 → 108 rows (30s poll) |

---

## Official Documentation (Extracted from Confluent Docs)

### Source: https://docs.confluent.io/cloud/current/connectors/cc-snowflake-sink/cc-snowflake-sink.html

#### Authentication Methods
- **Private key authentication (default):** Uses RSA key pair for secure access. Supports both `SNOWPIPE` and `SNOWPIPE_STREAMING`.
- **OAuth 2.0:** Only supported via the Confluent Cloud UI — NOT available via CLI or REST API.

#### Ingestion Methods
| Method | Behaviour | Notes |
|---|---|---|
| `SNOWPIPE` (default) | Batch — buffers into files then loads | Lower cost, minutes latency |
| `SNOWPIPE_STREAMING` | Row-level streaming | Seconds latency, `snowflake.role.name` required |

#### Schematization (`snowflake.enable.schematization`)
- Only works with `SNOWPIPE_STREAMING`
- When `true`: connector **auto-creates the table** and maps Kafka field names directly to Snowflake column names. It will also `ALTER TABLE` to add new columns as the schema evolves.
- When `false` (default): all data lands in `RECORD_CONTENT` (VARIANT) and `RECORD_METADATA` (VARIANT) columns — no named columns.
- **Type:** boolean, **Default:** false
- ⚠️ Confluent docs show `TRUE` but the connector config validator requires lowercase `true`

#### Input Data Formats
| Format | Schema Registry Required? |
|---|---|
| `JSON` (schemaless) | ❌ No |
| `AVRO` | ✅ Yes |
| `JSON_SR` | ✅ Yes |
| `PROTOBUF` | ✅ Yes |
| `BYTES` | ❌ No |

#### Table Behaviour
- If the target table **does not exist**: connector creates it automatically
- If the target table **exists**: connector adds `RECORD_CONTENT` and `RECORD_METADATA` columns and requires all other columns to allow NULL values
- With schematization enabled: connector automatically manages column additions for new fields
- ⚠️ **Do NOT pre-create the target table** when using schematization — it causes schema compatibility errors

#### Topic-to-Table Name Mapping
- Default: topic name becomes table name (dots and hyphens → underscores)
- Override: `"snowflake.topic2table.map": "my_topic:my_table"`

#### Key Configuration Properties Reference
| Property | Type | Default | Description |
|---|---|---|---|
| `snowflake.url.name` | string | — | Snowflake account URL: `<account>.snowflakecomputing.com` |
| `snowflake.user.name` | string | — | Snowflake username |
| `snowflake.private.key` | password | — | RSA private key, base64 only, single line, no PEM headers |
| `snowflake.role.name` | string | — | Required for SNOWPIPE_STREAMING and OAuth |
| `snowflake.database.name` | string | — | Target Snowflake database |
| `snowflake.schema.name` | string | — | Target Snowflake schema |
| `snowflake.ingestion.method` | string | `SNOWPIPE` | `SNOWPIPE` or `SNOWPIPE_STREAMING` |
| `snowflake.enable.schematization` | boolean | `false` | Auto column naming. Requires SNOWPIPE_STREAMING |
| `snowflake.topic2table.map` | string | — | Override topic→table mapping: `topic:table` |
| `snowflake.metadata.all` | boolean | `true` | If false, RECORD_METADATA column is empty |
| `tasks.max` | int | 1 | Set to number of Kafka partitions for parallelism |
| `input.data.format` | string | `JSON` | `JSON`, `AVRO`, `JSON_SR`, `PROTOBUF`, `BYTES` |

---

### Source: https://docs.snowflake.com/en/user-guide/key-pair-auth

#### RSA Key Pair Authentication — How It Works
1. Generate a private/public RSA key pair locally using `openssl`
2. Assign the **public key** to the Snowflake user via `ALTER USER SET RSA_PUBLIC_KEY='...'`
3. Provide the **private key** (base64, no headers) to the connector config
4. Snowflake verifies authentication by checking the submitted private key against the stored public key

#### Key Pair Generation Commands
```bash
# Generate 2048-bit RSA private key in PKCS8 unencrypted format
openssl genrsa 2048 | openssl pkcs8 -topk8 -nocrypt -out /tmp/sf_key.p8

# Derive the public key
openssl rsa -in /tmp/sf_key.p8 -pubout -out /tmp/sf_key.pub
```

#### Assign Public Key to User
```sql
-- Extract just the key body (no headers, no newlines) and assign
ALTER USER <username> SET RSA_PUBLIC_KEY='<base64_key_body>';
```

#### Key Rotation Support
- Snowflake supports up to 2 active public keys per user (`RSA_PUBLIC_KEY` and `RSA_PUBLIC_KEY_2`)
- By default, a rotated key remains valid for 24 hours
- Set `EXPIRE_ROTATED_KEY_PAIR_AFTER_HOURS=0` to expire immediately

---

## Complete Working Configuration

### Step 1 — Snowflake Setup (one-time)

```bash
# Install Snowflake CLI
brew install snowflake-cli

# Add connection (enter password interactively when prompted)
snow connection add \
  --connection-name lab \
  --account vrlnecq-xo34979 \
  --user CSAL \
  --warehouse COMPUTE_WH \
  --database SNOWFLAKE \
  --schema PUBLIC

# Verify
snow sql -q "SELECT CURRENT_USER(), CURRENT_ACCOUNT();" --connection lab \
  2>&1 | grep -v UserWarning | grep -v warn_incompatible

# Create target schema (database LAB already exists)
snow sql --connection lab -q "CREATE SCHEMA IF NOT EXISTS LAB.TEST;" \
  2>&1 | grep -v UserWarning | grep -v warn_incompatible

# DO NOT create the target table — connector auto-creates it
```

### Step 2 — Generate RSA Key Pair (one-time per Snowflake account)

```bash
# Generate key pair
openssl genrsa 2048 | openssl pkcs8 -topk8 -nocrypt -out /tmp/sf_key.p8
openssl rsa -in /tmp/sf_key.p8 -pubout -out /tmp/sf_key.pub

# Assign public key to CSAL user
PUB_KEY=$(cat /tmp/sf_key.pub | grep -v "PUBLIC KEY" | tr -d '\n')
snow sql --connection lab -q "ALTER USER CSAL SET RSA_PUBLIC_KEY='$PUB_KEY';" \
  2>&1 | grep -v UserWarning | grep -v warn_incompatible
```

### Step 3 — Provision Kafka Cluster & API Key

```bash
# Basic cluster = cheapest (~$0.135/CKU-hour vs $0.75 for Standard)
confluent kafka cluster create test-snowflake-cluster \
  --cloud aws --region us-east-2 --type basic

confluent kafka cluster use <CLUSTER_ID>
confluent api-key create --resource <CLUSTER_ID> --description "Snowflake Test Key"
confluent api-key use <API_KEY> --resource <CLUSTER_ID>
```

### Step 4 — Deploy Datagen Source Connector (JSON, no Schema Registry)

```bash
python3 - <<'EOF'
import json
config = {
  "name": "DatagenSource_test",
  "config": {
    "connector.class": "DatagenSource",
    "name": "DatagenSource_test",
    "kafka.auth.mode": "KAFKA_API_KEY",
    "kafka.api.key": "<KAFKA_API_KEY>",
    "kafka.api.secret": "<KAFKA_API_SECRET>",
    "kafka.topic": "test_stream",
    "output.data.format": "JSON",
    "quickstart": "ORDERS",
    "tasks.max": "1"
  }
}
with open("/tmp/connector-datagen-test.json", "w") as f:
    json.dump(config, f, indent=2)
print("Written")
EOF

confluent connect cluster create \
  --config-file /tmp/connector-datagen-test.json \
  --cluster <CLUSTER_ID>
```

**Wait for topic to appear:**
```bash
confluent kafka topic list --cluster <CLUSTER_ID>
# test_stream should appear within ~30 seconds
```

### Step 5 — Build & Deploy Snowflake Sink Connector

```bash
python3 - <<'EOF'
import json

with open('/tmp/sf_key.p8') as f:
    lines = f.readlines()
private_key = ''.join(l.strip() for l in lines if 'PRIVATE KEY' not in l)

config = {
  "name": "SnowflakeSink_test",
  "config": {
    "connector.class": "SnowflakeSink",
    "name": "SnowflakeSink_test",
    "kafka.auth.mode": "KAFKA_API_KEY",
    "kafka.api.key": "<KAFKA_API_KEY>",
    "kafka.api.secret": "<KAFKA_API_SECRET>",
    "topics": "test_stream",
    "input.data.format": "JSON",
    "snowflake.url.name": "vrlnecq-xo34979.snowflakecomputing.com",
    "snowflake.user.name": "CSAL",
    "snowflake.private.key": private_key,
    "snowflake.role.name": "ACCOUNTADMIN",
    "snowflake.database.name": "LAB",
    "snowflake.schema.name": "TEST",
    "snowflake.ingestion.method": "SNOWPIPE_STREAMING",
    "snowflake.enable.schematization": "true",
    "tasks.max": "1"
  }
}
with open("/tmp/connector-snowflake-sink-test.json", "w") as f:
    json.dump(config, f, indent=2)
print("Config written")
EOF

confluent connect cluster create \
  --config-file /tmp/connector-snowflake-sink-test.json \
  --cluster <CLUSTER_ID>
```

### Step 6 — Verify Data in Snowflake (~2 minutes after connector starts)

```bash
# Check row count
snow sql --connection lab -q "SELECT COUNT(*) FROM LAB.TEST.TEST_STREAM;" \
  2>&1 | grep -v UserWarning | grep -v warn_incompatible

# View actual rows with named columns
snow sql --connection lab -q "SELECT * FROM LAB.TEST.TEST_STREAM LIMIT 5;" \
  2>&1 | grep -v UserWarning | grep -v warn_incompatible

# Confirm it's growing (run twice 30 seconds apart)
snow sql --connection lab -q "SELECT COUNT(*) FROM LAB.TEST.TEST_STREAM;" \
  2>&1 | grep -v UserWarning | grep -v warn_incompatible
```

**Expected output once working:**
```
RECORD_METADATA | ITEMID  | ORDERID | ORDERUNITS | ADDRESS         | ORDERTIME
{...kafka meta} | Item_1  | 0       | 0.25       | {"city":...}    | 1491390653141
{...kafka meta} | Item_86 | 1       | 8.83       | {"city":...}    | 1508412889680
```

### Step 7 — Teardown

```bash
# Delete connectors
confluent connect cluster delete <DATAGEN_ID> --cluster <CLUSTER_ID> --force
confluent connect cluster delete <SINK_ID> --cluster <CLUSTER_ID> --force

# Delete cluster (stops all billing)
confluent kafka cluster delete <CLUSTER_ID> --force

# Optional: clean up Snowflake
snow sql --connection lab -q "DROP SCHEMA IF EXISTS LAB.TEST CASCADE;" \
  2>&1 | grep -v UserWarning | grep -v warn_incompatible
```

---

## All Errors Encountered & Resolutions

| # | Error | Root Cause | Resolution | Status |
|---|---|---|---|---|
| 1 | `snowflake.enable.schematization: Invalid value 'TRUE'` | Docs show `TRUE` but validator requires lowercase | Use `"true"` (lowercase) | ✅ Fixed |
| 2 | Previous session: `snowflake.role.name` required validation error | Required for `SNOWPIPE_STREAMING` mode | Always include `"snowflake.role.name": "ACCOUNTADMIN"` | ✅ Fixed |
| 3 | Previous session: `sed` fails on macOS with private key | BSD sed breaks on `/` and `+` in base64 | Use Python to build JSON config | ✅ Fixed |
| 4 | Previous session: "table does not have compatible schema" | Pre-created table conflicts with connector's auto-schema | Never pre-create table — let connector auto-create it | ✅ Fixed |
| 5 | Previous session: "Insufficient privileges" with custom role | Custom role missing `CREATE TABLE`, `CREATE STAGE`, `CREATE PIPE` | Use ACCOUNTADMIN directly — no custom role | ✅ Fixed |

---

## Cost Reference

| Resource | Type | Rate |
|---|---|---|
| Kafka Basic cluster | 1 CKU | $0.135/CKU-hour |
| Kafka Standard cluster | 1 CKU | $0.75/CKU-hour |
| Datagen connector | per task | ~$0.035/task-hour |
| Snowflake Sink connector | per task | ~$0.035/task-hour |
| Flink CFU | per CFU | $0.21/CFU-hour |
| **Full test (Basic + 2 connectors, 1 hour)** | | **~$0.21** |
| **Full lab (Standard + Flink 3 CFU + 3 connectors, 1 hour)** | | **~$1.60** |

> ⚠️ Kafka clusters bill continuously — delete when done. Flink pools bill per CFU — suspend materialized tables first to bring CFU to 0, then delete pool.

---

## Key Rules Summary

1. `snowflake.enable.schematization` → `"true"` (lowercase) — docs say `TRUE` but validator rejects it
2. `snowflake.role.name` → always required when using `SNOWPIPE_STREAMING`
3. Never pre-create the target table — connector auto-creates with correct schema
4. Use ACCOUNTADMIN directly — no custom role/user needed for a lab
5. Use Python to build JSON config on macOS — never `sed` (BSD sed breaks on base64 characters)
6. Use `JSON` format for simplest setup — no Schema Registry required
7. Use `basic` cluster type for testing — 5.5x cheaper than `standard`
8. Snowpipe Streaming latency is ~1–2 minutes — don't check Snowflake immediately
9. Always confirm Kafka topic exists before deploying sink connector
10. Never delete/recreate connector on config errors — use `confluent connect cluster update` instead
