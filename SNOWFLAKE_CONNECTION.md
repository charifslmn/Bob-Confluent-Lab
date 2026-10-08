# Snowflake Sink Connector — Setup Log

This file is a complete record of everything attempted, what failed, what worked, and all lessons learned when connecting Confluent Cloud to Snowflake via the Snowflake Sink Connector. It will directly inform the agent skill file and the lab instructions.

---

## Final Working Architecture

```
Confluent Cloud
  └── Kafka Topic: trades_forecast (AVRO)
        └── Snowflake Sink Connector (fully managed, Confluent Cloud)
              ├── ingestion method: SNOWPIPE_STREAMING
              ├── schematization: true (connector auto-creates table)
              ├── auth: RSA key pair assigned to CSAL (ACCOUNTADMIN)
              └── Snowflake: LAB.CONFLUENT.TRADES_FORECAST (auto-created by connector)
```

Data flows **one way**: Confluent → Snowflake. The connector continuously polls the `trades_forecast` Kafka topic and inserts each new forecast row into the Snowflake table in real time. Snowpipe Streaming has a ~1 minute latency before rows appear in Snowflake.

---

## Tools Required

| Tool | Purpose | Install |
|---|---|---|
| `confluent` CLI | Deploy the sink connector | Already installed |
| `snow` CLI | Run SQL on Snowflake without the UI | `brew install snowflake-cli` |
| `openssl` | Generate RSA key pair for connector auth | Pre-installed on macOS |

---

## Step 1 — Install Snowflake CLI ✅

```bash
brew install snowflake-cli
snow --version
```

**Result:** `Snowflake CLI version: 3.28.0` ✅

> ⚠️ **Known warning (harmless):** After every `snow` command you will see:
> ```
> UserWarning: You have an incompatible version of 'pyarrow' installed (25.0.1)
> ```
> This does not affect functionality. Ignore it. Suppress it in output with `2>&1 | grep -v UserWarning | grep -v warn_incompatible`.

---

## Step 2 — Connect Snowflake CLI to Account ✅

The Snowflake account identifier comes from the URL when logged in:
`https://app.snowflake.com/vrlnecq/xo34979` → account identifier = `vrlnecq-xo34979`

You can also find it by clicking your account name in the bottom-left of the Snowflake UI.

```bash
snow connection add \
  --connection-name lab \
  --account vrlnecq-xo34979 \
  --user CSAL \
  --warehouse COMPUTE_WH \
  --database SNOWFLAKE \
  --schema PUBLIC
```

When prompted, enter the Snowflake password **interactively** (it will not echo). Then verify:

```bash
snow sql -q "SELECT CURRENT_USER(), CURRENT_ACCOUNT();" --connection lab
```

**Result:** Returns username `CSAL` and account ID ✅

> ⚠️ **Gotcha 1:** Use `--database SNOWFLAKE --schema PUBLIC` for the initial connection setup, NOT `--database LAB`. The `LAB` database doesn't exist yet at this point. `snow connection test` will error if the default database/schema doesn't exist. The `snow sql` commands work correctly regardless.

> ⚠️ **Gotcha 2:** Do not use `--no-interactive` when adding the connection if a password is needed — it silently skips the password prompt and the connection will fail authentication later.

> ⚠️ **Gotcha 3:** `snow connection test` is unreliable as a connectivity check. Use `snow sql -q "SELECT CURRENT_USER();"` instead — if it returns a result, the connection works.

---

## Step 3 — Create Database and Schema ✅

```bash
snow sql --connection lab -q "
CREATE DATABASE IF NOT EXISTS LAB;
CREATE SCHEMA IF NOT EXISTS LAB.CONFLUENT;
" 2>&1 | grep -v UserWarning | grep -v warn_incompatible
```

**Result:** Database `LAB` and schema `CONFLUENT` created ✅

> ℹ️ **Do NOT pre-create the target table.** See Step 7 for why.

---

## Step 4 — Generate RSA Key Pair ✅

> ⚠️ **Critical:** The Confluent Snowflake Sink Connector does **NOT** support username + password authentication. RSA key pair is the only supported auth method. There is no workaround.

```bash
# Generate 2048-bit RSA private key in PKCS8 unencrypted format
openssl genrsa 2048 | openssl pkcs8 -topk8 -nocrypt -out /tmp/snowflake_connector_key.p8

# Derive the public key
openssl rsa -in /tmp/snowflake_connector_key.p8 -pubout -out /tmp/snowflake_connector_key.pub
```

**Result:** Key pair generated at `/tmp/snowflake_connector_key.p8` and `/tmp/snowflake_connector_key.pub` ✅

---

## Step 5 — Assign Public Key to CSAL User ✅

Assign directly to the ACCOUNTADMIN user `CSAL`. No separate connector user needed.

```bash
PUB_KEY=$(cat /tmp/snowflake_connector_key.pub | grep -v "PUBLIC KEY" | tr -d '\n')
snow sql --connection lab -q "ALTER USER CSAL SET RSA_PUBLIC_KEY='$PUB_KEY';" \
  2>&1 | grep -v UserWarning | grep -v warn_incompatible
```

**Result:** Public key assigned to `CSAL` ✅

> ⚠️ **Gotcha:** If you originally assigned the key to a different user (e.g. `CONFLUENT_USER`) and then switch to `CSAL`, you must re-run the `ALTER USER` command for `CSAL`. The key is user-specific.

---

## Step 6 — Build Connector Config JSON ✅

> ⚠️ **Gotcha:** Do NOT use `sed` to inject the private key on macOS. BSD sed breaks on the `/` and `+` characters in base64-encoded keys. Use Python instead.

```bash
python3 - <<'EOF'
import json

with open('/tmp/snowflake_connector_key.p8') as f:
    lines = f.readlines()
private_key = ''.join(l.strip() for l in lines if 'PRIVATE KEY' not in l)

config = {
  "name": "SnowflakeSink_trades_forecast",
  "config": {
    "connector.class": "SnowflakeSink",
    "name": "SnowflakeSink_trades_forecast",
    "kafka.auth.mode": "KAFKA_API_KEY",
    "kafka.api.key": "${KAFKA_API_KEY}",
    "kafka.api.secret": "${KAFKA_API_SECRET}",
    "topics": "trades_forecast",
    "input.data.format": "AVRO",
    "snowflake.url.name": "<ACCOUNT_ID>.snowflakecomputing.com",
    "snowflake.user.name": "CSAL",
    "snowflake.private.key": private_key,
    "snowflake.role.name": "ACCOUNTADMIN",
    "snowflake.database.name": "LAB",
    "snowflake.schema.name": "CONFLUENT",
    "snowflake.ingestion.method": "SNOWPIPE_STREAMING",
    "snowflake.enable.schematization": "true",
    "tasks.max": "1"
  }
}

with open("Lab CLI/configs/connector-snowflake-sink.json", "w") as f:
    json.dump(config, f, indent=2)
print("Config written")
EOF
```

**Result:** `Lab CLI/configs/connector-snowflake-sink.json` written ✅

> ⚠️ **Gotcha:** `snowflake.enable.schematization` must be lowercase `"true"` — NOT `"TRUE"`. The connector config validator is case-sensitive and will reject `"TRUE"` with a validation error.

> ⚠️ **Gotcha:** `snowflake.role.name` is **required** when `snowflake.ingestion.method=SNOWPIPE_STREAMING`. Omitting it causes a validation error on connector creation. It is optional when using the default Snowpipe method.

---

## Step 7 — Verify Kafka Topic Exists Before Deploying ✅

> ⚠️ **Critical:** Deploy the connector ONLY AFTER `trades_forecast` appears in the Kafka topic list. The Flink materialized table DDL returns `COMPLETED` immediately, but the backing Kafka topic is not created until the continuous streaming job starts and produces data. Deploying too early causes:
> *"Topic(s) 'trades_forecast' doesn't exist"*

```bash
# Wait until trades_forecast appears
confluent kafka topic list --cluster <CLUSTER_ID>
```

Expected output when ready:
```
sample_data_stock_trades
sample_data_users
trades_enriched          ← must exist
trades_forecast          ← must exist before deploying connector
users_keyed              ← must exist
```

Also confirm Flink CFU > 0 to verify streaming jobs are actually running:
```bash
confluent flink compute-pool describe <POOL_ID>
# Current CFU should be 3 (one per materialized table)
```

> ⚠️ **Gotcha — Do NOT use `--wait` when creating Flink materialized tables.** It blocks indefinitely because materialized tables are continuous streaming jobs that never terminate. Always create without `--wait`, then poll topic list and CFU to confirm readiness.

---

## Step 8 — Deploy the Connector ✅

```bash
confluent connect cluster create \
  --config-file "Lab CLI/configs/connector-snowflake-sink.json" \
  --cluster <CLUSTER_ID>
```

**Result:** Connector `lcc-k86y66p` created, status `RUNNING`, task `RUNNING` ✅

> ⚠️ **Gotcha — Do NOT delete and recreate the connector on every config error.** Use update instead:
> ```bash
> confluent connect cluster update <CONNECTOR_ID> \
>   --config-file configs/connector-snowflake-sink.json \
>   --cluster <CLUSTER_ID>
> ```
> Deleting and recreating wastes time and credits. Only create once.

---

## Step 9 — Verify Data Arriving in Snowflake ⏳ (Partially Verified)

The connector reached `RUNNING` status and the table was auto-created by the connector in `LAB.CONFLUENT` with two columns:
- `RECORD_CONTENT` (VARIANT) — the raw record payload
- `RECORD_METADATA` (VARIANT) — Kafka metadata (offset, partition, topic, timestamp)

With `snowflake.enable.schematization=true` the connector should evolve the table to flat named columns after processing the first batch of records.

The `trades_forecast` topic was confirmed to have live data (verified via `confluent kafka topic consume`). Snowpipe Streaming has a ~1 minute buffer latency before rows appear in Snowflake.

**Verification query (run after ~2 minutes):**
```bash
snow sql --connection lab -q "SELECT COUNT(*) FROM LAB.CONFLUENT.TRADES_FORECAST;" \
  2>&1 | grep -v UserWarning | grep -v warn_incompatible
```

**Once schematization kicks in, query with named columns:**
```bash
snow sql --connection lab -q "
SELECT SYMBOL, TS, CURRENT_COUNT, FORECAST_COUNT, UPPER_BOUND
FROM LAB.CONFLUENT.TRADES_FORECAST
ORDER BY TS DESC
LIMIT 10;
" 2>&1 | grep -v UserWarning | grep -v warn_incompatible
```

> ⏳ **Status:** Connector was RUNNING and table was auto-created. Data flow was not fully confirmed before resources were torn down. Need to verify row count > 0 on next run.

---

## Step 10 — Teardown (When Done) ✅

Resources were fully deleted to avoid ongoing charges. Correct teardown order:

```bash
# 1. Suspend Flink materialized tables first (stops CFU billing)
confluent flink statement create suspend-forecast \
  --database <CLUSTER_ID> --compute-pool <POOL_ID> \
  --cloud aws --region us-east-2 \
  --sql "ALTER MATERIALIZED TABLE trades_forecast SUSPEND;"

confluent flink statement create suspend-enriched \
  --database <CLUSTER_ID> --compute-pool <POOL_ID> \
  --cloud aws --region us-east-2 \
  --sql "ALTER MATERIALIZED TABLE trades_enriched SUSPEND;"

confluent flink statement create suspend-users \
  --database <CLUSTER_ID> --compute-pool <POOL_ID> \
  --cloud aws --region us-east-2 \
  --sql "ALTER MATERIALIZED TABLE users_keyed SUSPEND;"

# 2. Pause all connectors
confluent connect cluster pause <CONNECTOR_ID> --cluster <CLUSTER_ID>

# 3. Delete connectors
confluent connect cluster delete <CONNECTOR_ID> --cluster <CLUSTER_ID> --force

# 4. Delete Flink compute pool
confluent flink compute-pool delete <POOL_ID> --force

# 5. Delete Kafka cluster (cannot be paused — only delete stops billing)
confluent kafka cluster delete <CLUSTER_ID> --force
```

> ⚠️ **Important:** Kafka clusters cannot be paused — they bill continuously while provisioned. The only way to stop billing is to delete the cluster. The Flink compute pool also bills per CFU — suspend materialized tables to bring CFU to 0 before deleting.

---

## Complete Error Log

| # | Error | Root Cause | Fix | Worked? |
|---|---|---|---|---|
| 1 | `snow connection test` fails: "schema PUBLIC does not exist" | Default `--database LAB` set before LAB database was created | Use `--database SNOWFLAKE --schema PUBLIC` for initial connection | ✅ Fixed |
| 2 | `sed` fails on macOS injecting private key into JSON | BSD sed can't handle `/` and `+` in base64 key strings | Use Python string replacement | ✅ Fixed |
| 3 | Connector FAILED: "Topic 'trades_forecast' doesn't exist" | Deployed connector before Flink streaming job had created the topic | Always run `confluent kafka topic list` and confirm topic exists first | ✅ Fixed |
| 4 | Connector FAILED: "table does not have compatible schema" (1st) | Pre-created table used `NUMBER` for nullable double fields | Inspect AVRO schema via Schema Registry first; use `FLOAT` for nullable doubles | ✅ Fixed (then superseded by #7) |
| 5 | Flink CFU = 0 after creating materialized tables | Used `--wait` flag which caused DDL to appear completed but jobs didn't start | Create materialized tables without `--wait`; confirm CFU > 0 | ✅ Fixed |
| 6 | Connector FAILED: "Insufficient privileges to create table on schema" | Separate `CONFLUENT_CONNECTOR_ROLE` missing `CREATE TABLE`, `CREATE STAGE`, `CREATE PIPE` grants | Abandoned separate role — switched to ACCOUNTADMIN user directly | ✅ Fixed |
| 7 | Connector FAILED: "table does not have compatible schema" (2nd) | Pre-created table exists — connector cannot reconcile it with auto-created schema | Drop any pre-existing target table; let connector auto-create it | ✅ Fixed |
| 8 | `CONFLUENT_USER` / `CONFLUENT_CONNECTOR_ROLE` permission chain kept failing | Too many moving parts for a lab setup | Dropped entirely — use ACCOUNTADMIN user (`CSAL`) directly | ✅ Fixed |
| 9 | Connector deleted and recreated many times on each config error | Should have used `confluent connect cluster update` instead | Use update command to fix config in place | ✅ Noted for next time |
| 10 | `"snowflake.enable.schematization": "TRUE"` rejected by connector | Config validator is case-sensitive | Use lowercase `"true"` | ✅ Fixed |
| 11 | `snowflake.role.name` missing validation error with SNOWPIPE_STREAMING | Required field for Snowpipe Streaming mode, not documented prominently | Always include `"snowflake.role.name": "ACCOUNTADMIN"` | ✅ Fixed |
| 12 | Snowflake table auto-created with only `RECORD_CONTENT` + `RECORD_METADATA` columns instead of flat schema | Schematization needs time to process first batch and evolve the table | Wait ~2 minutes after connector starts before querying with named columns | ⏳ Not yet verified — need to confirm on next run |
| 13 | Orphaned Snowflake objects (stage + 2 pipes) left in `LAB.CONFLUENT` after deleting a Confluent connector that used standard `SNOWPIPE` | Confluent connector delete only removes the Confluent-side resource — Snowflake objects (stage, pipes) are NOT auto-deleted | Manually drop with `DROP PIPE` and `DROP STAGE` in Snowflake after deleting any connector that used `SNOWPIPE` mode | ✅ Fixed — cleaned up manually |
| 14 | Pipe lineage graph in Snowflake UI showed `TRADES_FORECAST` as a destination but the table was empty | Orphaned pipes from old connector still existed in Snowflake — pipe showed "Running" but had no source feeding it | Delete orphaned pipes/stages immediately when tearing down `SNOWPIPE` connectors | ✅ Fixed |

---

## What DID Work (Confirmed ✅)

1. **Snowflake CLI install** via `brew install snowflake-cli` ✅
2. **Snowflake CLI connection** to `vrlnecq-xo34979` using `snow sql` ✅
3. **Database and schema creation** via `snow sql` ✅
4. **RSA key pair generation** via `openssl` ✅
5. **Assigning RSA public key to CSAL user** via `ALTER USER CSAL SET RSA_PUBLIC_KEY` ✅
6. **Building connector config JSON** via Python ✅
7. **Connector deployment** via `confluent connect cluster create` ✅
8. **Connector reached RUNNING status** with task RUNNING ✅
9. **Table auto-created by connector** in `LAB.CONFLUENT` ✅
10. **Live data confirmed in `trades_forecast` Kafka topic** via `confluent kafka topic consume` ✅
11. **Full teardown** — all resources deleted cleanly, zero charges ✅

## ✅ AVRO Verification Complete (2026-10-07)

All items verified against `LAB.TEST_AVRO.TRADES_FORECAST`:

- [x] Row count > 0 — **94 rows** on first poll (~90s after connector RUNNING) ✅
- [x] Schematization evolved table to flat named columns: `SYMBOL`, `TS`, `CURRENT_COUNT`, `FORECAST_COUNT`, `UPPER_BOUND` ✅
- [x] Live streaming confirmed — **94 → 101 → 108 rows** across 30-second poll intervals ✅
- [x] Full named-column query confirmed: `SELECT SYMBOL, TS, CURRENT_COUNT, FORECAST_COUNT, UPPER_BOUND FROM LAB.TEST_AVRO.TRADES_FORECAST ORDER BY TS DESC LIMIT 10` returned real ML_FORECAST data ✅
- [x] Full teardown confirmed: all connectors deleted, Flink CFU→0, pool deleted, cluster deleted, zero charges ✅
- [ ] Screenshot of Snowflake UI showing live data — still needed for README_V2.md

---

## ✅ DEFINITIVE WORKING SETUP (Verified 2026-10-06)

After all the failures above, a simplified test was run to confirm end-to-end streaming works. The test used:
- **Basic Kafka cluster** (cheaper than Standard)
- **JSON format** (no Schema Registry, simpler than AVRO)
- **ORDERS Datagen quickstart** (simple flat + nested JSON)
- **SNOWPIPE_STREAMING + schematization enabled**
- **ACCOUNTADMIN user (CSAL) directly** — no custom role

### Result
- Connector reached `RUNNING` status immediately ✅
- Table `TEST_STREAM` auto-created in `LAB.TEST` by the connector ✅
- **449 rows** on first check (~2 min after start) ✅
- **577 rows** 30 seconds later — **live streaming confirmed** ✅
- Named columns auto-created: `RECORD_METADATA`, `ITEMID`, `ORDERID`, `ORDERUNITS`, `ADDRESS`, `ORDERTIME` ✅

### The Exact Working Config
```json
{
  "connector.class": "SnowflakeSink",
  "kafka.auth.mode": "KAFKA_API_KEY",
  "topics": "test_stream",
  "input.data.format": "JSON",
  "snowflake.url.name": "vrlnecq-xo34979.snowflakecomputing.com",
  "snowflake.user.name": "CSAL",
  "snowflake.private.key": "<BASE64_PRIVATE_KEY_NO_HEADERS>",
  "snowflake.role.name": "ACCOUNTADMIN",
  "snowflake.database.name": "LAB",
  "snowflake.schema.name": "TEST",
  "snowflake.ingestion.method": "SNOWPIPE_STREAMING",
  "snowflake.enable.schematization": "true",
  "tasks.max": "1"
}
```

### The 3 Critical Fixes That Made It Work
1. **`"snowflake.enable.schematization": "true"`** — lowercase, not `"TRUE"` as docs show
2. **`"snowflake.role.name": "ACCOUNTADMIN"`** — required for SNOWPIPE_STREAMING, easy to miss
3. **No pre-created table** — drop any existing table and let the connector create it

### What's Different vs the AVRO/Flink lab approach
For the full lab (with `trades_forecast` in AVRO from Flink), the same config pattern applies. The only differences are:
- `"input.data.format": "AVRO"` instead of `"JSON"`
- `"topics": "trades_forecast"` and `"snowflake.schema.name": "CONFLUENT"`
- Schema Registry is required for AVRO — the Confluent environment already has it enabled

> 📄 See `SNOWFLAKE_SINK_TEST.md` for the full step-by-step guide with all documentation extracted from official sources.

---

## Key Rules for Agent Skill File

1. **Never pre-create the target Snowflake table.** Let the connector auto-create it with `snowflake.enable.schematization=true`.
2. **Use ACCOUNTADMIN user directly.** No separate role or user needed for a lab.
3. **RSA key pair required.** Password auth is not supported. Generate with `openssl`, assign public key to the Snowflake user, embed private key in connector config.
4. **Use Python to build the connector config JSON** — never `sed` on macOS.
5. **Always confirm topic exists before deploying connector.** Run `confluent kafka topic list` and verify `trades_forecast` is present.
6. **Always confirm CFU > 0 before deploying connector.** Verifies Flink streaming jobs are actually running.
7. **Create materialized tables WITHOUT `--wait`.** It blocks indefinitely.
8. **`snowflake.ingestion.method=SNOWPIPE_STREAMING` requires `snowflake.role.name`.** Always include it.
9. **`snowflake.enable.schematization` must be lowercase `"true"`.** Not `"TRUE"`.
10. **Never delete and recreate a connector on config errors.** Use `confluent connect cluster update` instead.
11. **Kafka clusters cannot be paused — only deleted.** To stop all charges, delete the cluster.
12. **Snowpipe Streaming has ~1 minute latency.** Don't check Snowflake immediately after connector starts — wait at least 2 minutes.
13. **`SNOWPIPE_STREAMING` vs `SNOWPIPE` are two different modes — both valid.** `SNOWPIPE` (default) writes to a Snowflake Stage then copies via a Pipe — creates visible stage/pipe objects in Snowflake UI. `SNOWPIPE_STREAMING` uses the Streaming Ingest SDK — writes rows directly, no stage, no pipe, seconds latency, and is the only mode that supports schematization. Always use `SNOWPIPE_STREAMING` for this lab.
14. **When using `SNOWPIPE` mode, deleting the Confluent connector does NOT clean up Snowflake objects.** The stage and pipes created in Snowflake remain as orphans and must be manually dropped with `DROP PIPE` / `DROP STAGE` to avoid confusion. `SNOWPIPE_STREAMING` leaves no Snowflake objects behind on teardown.
