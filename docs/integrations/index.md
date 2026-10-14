# Integrations

rivers connects to the tools around your pipelines. Each integration has a guide and an API reference. Install the extra in the last column to use it.

## Ingestion

| Integration | What it does | Extra |
|---|---|---|
| [Airbyte](../guides/airbyte.md) | Runs Airbyte connection syncs as multi-assets, one output per stream. | — |
| [dlt](../guides/dlt.md) | Loads dlt sources as multi-assets, one output per resource. | `rivers[dlt]` |
| [Sling](../guides/sling.md) | Runs Sling replications as multi-assets, one output per stream. | `rivers[sling]` |

## Transformation and compute

| Integration | What it does | Extra |
|---|---|---|
| [dbt](../guides/dbt.md) | Makes dbt models, seeds, and snapshots into assets, and runs dbt to build them. | `rivers[dbt]` |
| [Databricks](../guides/databricks.md) | Runs Databricks jobs as assets, one asset per job or one output per task. | `rivers[databricks]` |
| [DuckDB](../guides/duckdb.md) | Shares DuckDB connections as resources and stores asset outputs in DuckDB. | `rivers[duckdb]` |

## Table formats

| Integration | What it does | Extra |
|---|---|---|
| [Delta Lake](../guides/delta-lake.md) | Stores asset outputs as Delta tables, with partitioned writes and MERGE. | `rivers[delta]` |
| [Iceberg](../guides/iceberg.md) | Stores asset outputs as Iceberg tables, with upserts and schema evolution. | `rivers[iceberg]` |

## Validation

| Integration | What it does | Extra |
|---|---|---|
| [dataframely](../guides/dataframely.md) | Checks polars outputs against the dataframely schema in the return hint. | `rivers[dataframely]` |
| [pandera](../guides/pandera.md) | Checks frame outputs against the pandera schema in the return hint. | `rivers[pandera]` |

## Alerts and monitoring

| Integration | What it does | Extra |
|---|---|---|
| [Datadog](../guides/datadog.md) | Sends events when assets or runs fail, and gives a client for your own metrics. | `rivers[datadog]` |
| [Slack](../guides/slack.md) | Posts a message when assets or runs fail. | `rivers[slack]` |
