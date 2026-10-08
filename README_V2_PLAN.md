# README_V2 — What It Is & What We're Building

## What is README_V2.md?

`README_V2.md` is an **extended version of the original lab** (`README.md`). It contains everything from the original lab (Steps 1–7) plus **one new step: streaming the `trades_forecast` output live into Snowflake** using the Confluent Snowflake Sink Connector.

The file lives at `Lab CLI/README_V2.md` and is a standalone lab document — participants can run it end-to-end without needing any other file.

---

## What the Original Lab Covers (Steps 1–7, unchanged)

| Step | What it does |
|---|---|
| 1 | Login to Confluent Cloud + find lab environment |
| 2 | Provision Kafka cluster + API keys |
| 3 | Deploy Datagen Source connectors (Users + Stock Trades, AVRO) |
| 4 | Provision Apache Flink compute pool |
| 5 | Build the Flink pipeline (3 materialized tables: `users_keyed`, `trades_enriched`, `trades_forecast`) |
| 6 | Query live enriched & forecast streams |
| 7 | Cleanup & teardown |

---

## What README_V2.md Adds — New Step 6: Stream to Snowflake

A new **Step 6** is inserted between the existing Step 5 (Flink pipeline) and Step 6 (Query), renumbering the query step to 7 and cleanup to 8.

The new step guides the participant through connecting Confluent Cloud to Snowflake so that every row produced by `ML_FORECAST` in the `trades_forecast` topic is streamed live into a Snowflake table — proving the full end-to-end pipeline.

### New Step 6 Sub-steps

| Sub-step | What the participant does | Who runs the command |
|---|---|---|
| **6.1** Sign up for Snowflake free trial | Click link, create account | Participant (manual) |
| **6.2** Install Snowflake CLI (`snow`) | Give Bob a prompt | **Bob** runs `brew install snowflake-cli` |
| **6.3** Connect Snow CLI to account | Give Bob a prompt → Bob prints the command → participant runs it with their password | **Participant** runs `snow connection add` (password is interactive) |
| **6.4** Verify connection | Give Bob a prompt | **Bob** runs `snow sql "SELECT CURRENT_USER()"` |
| **6.5** Create Snowflake database + schema | Give Bob a prompt | **Bob** runs `snow sql "CREATE DATABASE LAB; CREATE SCHEMA LAB.CONFLUENT;"` |
| **6.6** Generate RSA key pair + assign to Snowflake user | Give Bob a prompt | **Bob** runs `openssl` commands + `snow sql "ALTER USER SET RSA_PUBLIC_KEY"` |
| **6.7** Deploy Snowflake Sink Connector | Give Bob a prompt with Snowflake account details | **Bob** builds config via Python + runs `confluent connect cluster create` |
| **6.8** Verify live data arriving in Snowflake | Give Bob a prompt | **Bob** runs `snow sql "SELECT COUNT(*)"` and `SELECT * LIMIT 10` |

---

## Design Principles (same as the rest of the lab)

- Every action is triggered by a **natural language prompt to Bob**
- Each sub-step has a `💬 Prompt Bob` block with the exact suggested prompt
- Each sub-step has a `⚙️ What Bob does under the hood` collapsible showing the CLI commands
- Each sub-step has a **UI Verification Checkpoint** with a screenshot of what the participant should see
- **Passwords are never handled by Bob** — any step requiring a password is clearly marked as "run this yourself" with a ⚠️ callout

---

## Why `SNOWPIPE_STREAMING` (not standard Snowpipe)

The connector is configured with `snowflake.ingestion.method=SNOWPIPE_STREAMING` because:
- It writes rows directly into Snowflake in seconds (no file buffer / stage / pipe)
- It supports `schematization` — the connector auto-creates the table with proper named columns (`SYMBOL`, `TS`, `CURRENT_COUNT`, `FORECAST_COUNT`, `UPPER_BOUND`)
- It leaves no orphaned Snowflake objects (stages/pipes) when torn down
- It is the only mode that was proven to work reliably in testing

---

## Screenshots Needed (still to capture)

All screenshots go in `Lab CLI/screenshots/` and are referenced in `README_V2.md`.

| Screenshot | What it shows | Status |
|---|---|---|
| `08-snowflake-signup.png` | Snowflake free trial signup page | ⏳ Needed |
| `09-connector-snowflake-running.png` | Confluent Cloud — Snowflake Sink connector RUNNING with task RUNNING | ⏳ Needed |
| `10-snowflake-table-created.png` | Snowflake UI — `LAB.CONFLUENT.TRADES_FORECAST` table visible in object browser | ⏳ Needed |
| `11-snowflake-live-data.png` | Snowflake SQL worksheet — `SELECT * FROM LAB.CONFLUENT.TRADES_FORECAST LIMIT 10` with real rows | ⏳ Needed |

To capture these, the full pipeline must be running:
1. Spin up Kafka cluster + Datagen connectors + Flink pool + 3 materialized tables
2. Deploy Snowflake Sink connector against `trades_forecast` topic
3. Wait ~90 seconds for first rows to arrive
4. Take screenshots in Confluent Cloud UI and Snowflake UI
5. Tear everything down

---

## Key Technical Facts (from verified testing)

- **Connector name in config:** `SnowflakeSink_trades_forecast`
- **Kafka topic:** `trades_forecast` (AVRO format, produced by Flink ML_FORECAST)
- **Snowflake target:** `LAB.CONFLUENT.TRADES_FORECAST` (auto-created by connector)
- **Auth:** RSA key pair — `openssl genrsa 2048 | openssl pkcs8 -topk8 -nocrypt` → assign public key to user → embed private key in connector config
- **`snowflake.enable.schematization`:** must be lowercase `"true"` (not `"TRUE"`)
- **`snowflake.role.name`:** required when using `SNOWPIPE_STREAMING` — use `"ACCOUNTADMIN"`
- **Schema Registry:** automatically available in the Confluent environment (ESSENTIALS package) — no extra setup needed for AVRO
- **Latency:** ~90 seconds from connector RUNNING to first rows in Snowflake
- **Growth rate:** slow initially (ML_FORECAST needs 10 training windows per symbol), then ~1–2 new rows per symbol per minute
- **Config must be built via Python** on macOS — never `sed` (BSD sed breaks on base64 key characters)

---

## Files Referenced

| File | Purpose |
|---|---|
| `Lab CLI/README_V2.md` | The lab document being built |
| `Lab CLI/README.md` | Original lab (Steps 1–7, unchanged) |
| `Lab CLI/SNOWFLAKE_SINK_TEST.md` | Full verified test reference — JSON and AVRO tests, live poll results, working configs |
| `Lab CLI/SNOWFLAKE_CONNECTION.md` | Complete error log, 14 gotchas, 14 key rules for the agent skill file |
| `Lab CLI/configs/connector-snowflake-sink.json` | Connector config template (with real private key — do not commit to public repo) |
| `Lab CLI/screenshots/` | All lab screenshots |

---

## Current Status

| Item | Status |
|---|---|
| `README_V2.md` created (copy of README.md) | ✅ Done |
| AVRO streaming fully tested and verified | ✅ Done — 94→108 rows confirmed |
| `SNOWFLAKE_SINK_TEST.md` updated with full results | ✅ Done |
| `SNOWFLAKE_CONNECTION.md` updated with all gotchas + rules | ✅ Done |
| Screenshots captured | ⏳ Needed — requires one more pipeline run |
| Step 6 written into README_V2.md | ⏳ Pending screenshots |
| Snowflake LAB database cleaned (fresh start) | ✅ Done |
| All Confluent resources deleted (zero charges) | ✅ Done |
