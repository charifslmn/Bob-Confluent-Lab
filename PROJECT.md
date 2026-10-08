# Lab CLI — CLI-Driven Trade Forecasting with Confluent Intelligence

## Project Overview

This lab is a **CLI-driven recreation** of the original UI-driven lab found in `Lab UI Driven/`. The end goal and use case are identical — build a real-time trade enrichment and forecasting pipeline on Confluent Cloud — but every step that was previously done by clicking through the Confluent Cloud console will instead be executed via the **Confluent CLI** (`confluent`).

The intent is to give practitioners a reproducible, scriptable path through the same lab, suitable for automation, CI/CD pipelines, or environments where a browser is not available.

---

## Use Case (same as UI lab)

An online trading platform streams stock trades continuously. The team wants two things live:

1. **Enrichment** — join each trade with user profile data (who is trading and where are they from).
2. **Forecast** — detect which stocks are heating up in activity using `ML_FORECAST`, so the platform can get ahead of a surge before it peaks.

Both are computed continuously inside Confluent Cloud using **Apache Kafka** (managed clusters), **Datagen Source Connectors**, and **Flink SQL**.

---

## Scope

This lab covers the same stages as the UI lab, translated to CLI commands:

| Stage | UI Lab | CLI Lab (this project) |
|---|---|---|
| Account setup | Browser sign-up | `confluent login` |
| Cluster creation | Console wizard | `confluent kafka cluster create` |
| Connector deployment | Console UI | `confluent connect cluster create` |
| Topic inspection | Console Messages tab | `confluent kafka topic consume` |
| Flink compute pool | Console wizard | `confluent flink compute-pool create` |
| Flink SQL (enrichment + forecast) | SQL Workspace editor | `confluent flink shell` / SQL files |
| Cleanup | Console delete buttons | `confluent` delete commands |

---

## Current Work — Governance & Tableflow Extension (branch: `testing-v1`)

The base lab (Steps 1–7 in `README.md`, plus the Snowflake sink in `README_V2.md`) does not yet showcase three Confluent capabilities. The current effort is to **extend the lab to cover them**, rebuild the whole pipeline end-to-end, and capture fresh screenshots.

| # | Gap | Plan | CLI / UI | Risk |
|---|---|---|---|---|
| 1 | **Schema Registry** — the Avro schemas behind each topic are never shown | New step 6.3: list subjects, describe latest schema of `trades_forecast-value`; screenshot the topic **Data contracts** tab. Optional: set BACKWARD compatibility to show schema evolution | Both (`confluent schema-registry ...`) | Low |
| 2 | **Stream Lineage** — the end-to-end graph is never shown | New step 6.4: open Stream Lineage on `trades_forecast` after the full pipeline (incl. Snowflake sink) is running; screenshot the graph | UI only | Low |
| 3 | **Tableflow** — exposing a topic as an Iceberg table for an external service (Snowflake) | New step 8: enable Tableflow on `trades_forecast` and compare it with the sink connector (no data copy vs. pushed rows). Cleanup moves to step 9 | Mostly CLI (`confluent tableflow topic enable`, `confluent tableflow catalog-integration create`) | Medium |

### Tableflow constraints found during research

- Topics on **Confluent Managed Storage do not sync to external catalogs** (e.g. Snowflake, Glue). Snowflake reads need **BYOS** (S3 bucket + AWS provider integration).
- **Snowflake Open Catalog is closed to new accounts**; the docs point new deployments to **Snowflake Horizon Catalog** federation (not yet verified in full).
- **Flink retract-changelog tables are not supported**; `ML_FORECAST` / upsert outputs must be tested.
- Tableflow freshness is ~5 minutes; Flink cannot query Iceberg tables.
- Two approaches under consideration: **Option A** (managed storage, show table + built-in Iceberg REST Catalog, no Snowflake read) and **Option B** (BYOS on S3 + Snowflake catalog integration, query from Snowflake).

### Execution plan

1. Rebuild the entire lab on Confluent Cloud (cluster, Datagen connectors, Flink pool, 3 materialized tables, Snowflake sink).
2. Spike Tableflow on `trades_forecast` to settle the changelog and storage questions, then pick Option A or B.
3. Capture screenshots: Schema Registry data contract, Stream Lineage graph, Tableflow status (and Snowflake query if Option B).
4. Write the new steps into the lab README, verify, then tear down all resources (zero charges).

---

## Planned Additions (post-initial build)

- Shell script to run the full lab end-to-end in one command
- Config file for region / cloud provider overrides
- Validation checks between steps (e.g. confirm connector is `RUNNING` before proceeding)
- Potential extension: export forecast results to an external sink

---

## Relationship to UI Lab

| | `Lab UI Driven/` | `Lab CLI/` (this project) |
|---|---|---|
| Audience | Visual / beginner-friendly | CLI-comfortable / automation-oriented |
| Steps | Click-through console | `confluent` CLI commands |
| SQL content | Identical | Identical |
| Screenshots | Yes | Terminal output + UI verification |
| Scriptable | No | Yes (goal) |

The SQL queries (`users_keyed`, `trades_enriched`, `trades_forecast`) are **shared logic** — they will be the same in both labs.
