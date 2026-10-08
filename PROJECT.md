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
