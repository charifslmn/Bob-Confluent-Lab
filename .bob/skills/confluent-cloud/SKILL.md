---
name: confluent-cloud
description: Manage Confluent Cloud streaming infrastructure via the Confluent CLI — provision Kafka clusters, manage API keys, deploy managed connectors, configure Apache Flink compute pools, develop Flink SQL streaming pipelines (Materialized Tables, Temporal Joins, Windowing, ML functions), and inspect real-time topics.
---

# Confluent Cloud CLI & Stream Processing Guide

A practical guide for provisioning, operating, and building real-time stream processing pipelines on Confluent Cloud using the `confluent` CLI and Apache Flink.

---

## 1. Environment & Cluster Operations

### Discover Environments
```bash
confluent environment list
confluent environment use <ENVIRONMENT_ID_OR_NAME>
```

### Provision Kafka Cluster
```bash
confluent kafka cluster create <CLUSTER_NAME> \
  --cloud <aws|gcp|azure> \
  --region <REGION> \
  --type <standard|basic|enterprise|dedicated>
```

### Authentication & Context Management
```bash
confluent kafka cluster use <CLUSTER_ID>
confluent api-key create --resource <CLUSTER_ID> --description "<KEY_DESCRIPTION>"
confluent api-key use <API_KEY> --resource <CLUSTER_ID>
```

---

## 2. Managed Connectors Lifecycle

### Deploy Source/Sink Connector from JSON Configuration
```bash
confluent connect cluster create \
  --config-file <PATH_TO_CONFIG_JSON> \
  --cluster <CLUSTER_ID>
```

### Connector Inspection & Status
```bash
confluent connect cluster list --cluster <CLUSTER_ID>
confluent connect cluster describe <CONNECTOR_ID> --cluster <CLUSTER_ID>
```

### Update Existing Connector In-Place
```bash
confluent connect cluster update <CONNECTOR_ID> \
  --config-file <PATH_TO_CONFIG_JSON> \
  --cluster <CLUSTER_ID>
```

---

## 3. Apache Flink Compute Pools & Stream Processing (Flink SQL)

### Provision Compute Pool
```bash
confluent flink compute-pool create <POOL_NAME> \
  --cloud <CLOUD> \
  --region <REGION> \
  --max-cfu <MAX_CFU> \
  --wait
confluent flink compute-pool use <COMPUTE_POOL_ID>
```

### Streaming DDL Best Practice
> ⚠️ **Continuous Streaming Jobs:** Materialized tables and continuous streaming statements run indefinitely. Do **NOT** use `--wait` when executing streaming DDL statements, as the command will block awaiting job termination. Submit without `--wait` and monitor execution via CFU utilization:
> ```bash
> confluent flink compute-pool describe <COMPUTE_POOL_ID>
> ```

---

## 4. Flink SQL Streaming Patterns

### Pattern A: Keyed Lookup Table (Primary Key for Joins)
Creates a changelog lookup table with non-enforced primary key for low-latency point lookups:
```sql
CREATE MATERIALIZED TABLE <lookup_table_name> (
  <primary_key_column> STRING,
  <attribute_1> <TYPE>,
  <attribute_2> <TYPE>,
  PRIMARY KEY (<primary_key_column>) NOT ENFORCED
) WITH (
  'value.format' = 'avro-registry'
) AS SELECT
  <primary_key_column>, <attribute_1>, <attribute_2>
FROM <source_stream>;
```

### Pattern B: Temporal Join (Enriching Fact Streams with Lookup Data)
Enriches a real-time append stream against the historical state of a lookup table:
```sql
CREATE MATERIALIZED TABLE <enriched_table_name> AS
SELECT
  s.<field_1>, s.<field_2>, s.<field_3>,
  l.<lookup_field_1>, l.<lookup_field_2>,
  s.$rowtime AS event_time
FROM <fact_stream> s
JOIN <lookup_table_name> FOR SYSTEM_TIME AS OF s.$rowtime AS l
  ON s.<foreign_key_column> = l.<primary_key_column>;
```

### Pattern C: Tumbling Window Aggregation with Machine Learning
Aggregates time windows and applies built-in stream intelligence (such as `ML_FORECAST` or anomaly detection):
```sql
CREATE MATERIALIZED TABLE <forecast_table_name> AS
SELECT
  <partition_key>,
  window_end AS ts,
  current_metric,
  forecast[1].forecast_value AS forecast_value,
  forecast[1].upper_bound    AS upper_bound
FROM (
  SELECT
    <partition_key>,
    window_end,
    current_metric,
    ML_FORECAST(
      CAST(current_metric AS DOUBLE),
      window_end,
      JSON_OBJECT('minTrainingSize' VALUE 10, 'horizon' VALUE 5)
    ) OVER (
      PARTITION BY <partition_key>
      ORDER BY window_time
    ) AS forecast
  FROM (
    SELECT
      <partition_key>, window_start, window_end, window_time,
      COUNT(*) AS current_metric
    FROM TABLE(
      TUMBLE(TABLE <source_stream>, DESCRIPTOR($rowtime), INTERVAL '<N>' SECONDS)
    )
    GROUP BY <partition_key>, window_start, window_end, window_time
  )
)
WHERE CARDINALITY(forecast) >= 1;
```

---

## 5. Topic Inspection & Verification

### Consume Stream Data (AVRO / JSON)
```bash
confluent kafka topic list --cluster <CLUSTER_ID>
confluent kafka topic consume <TOPIC_NAME> -b --value-format avro
```
