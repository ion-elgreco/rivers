# Databricks

`rivers.integrations.databricks` runs [Databricks](https://www.databricks.com) jobs as rivers assets. `DatabricksResource` holds the workspace credentials. Its `job_asset` method makes an asset that runs a job of the workspace: one asset for the whole job, or one output for each task. A run starts the job, waits until the run ends, and records a materialization.

## Setup

```bash
pip install rivers[databricks]
```

`DatabricksResource` reads its fields from `DATABRICKS_*` environment variables when you do not give them:

```python
from rivers.integrations.databricks import DatabricksResource

databricks = DatabricksResource()  # DATABRICKS_HOST and DATABRICKS_TOKEN
databricks = DatabricksResource(
    host="https://dbc-1a2b3c4d-5e6f.cloud.databricks.com",
    client_id="...",
    client_secret="...",
)
```

Give the fields of one authentication method:

| Method | Fields | Environment variables |
|--------|--------|-----------------------|
| Personal access token | `token` | `DATABRICKS_TOKEN` |
| OAuth for a service principal | `client_id`, `client_secret` | `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET` |
| Azure service principal | `azure_client_id`, `azure_client_secret`, `azure_tenant_id` | `DATABRICKS_AZURE_CLIENT_ID`, `DATABRICKS_AZURE_CLIENT_SECRET`, `DATABRICKS_AZURE_TENANT_ID` |

`host` is the workspace URL (`DATABRICKS_HOST`). The fields go to the Databricks SDK. When a field is not set, the SDK uses its own [unified authentication](https://docs.databricks.com/aws/en/dev-tools/auth/unified-auth): other environment variables (for example `ARM_CLIENT_ID`, `ARM_CLIENT_SECRET` and `ARM_TENANT_ID`, or `DATABRICKS_CONFIG_PROFILE`), a profile in `~/.databrickscfg`, or the Azure or Databricks CLI.

`token`, `client_secret` and `azure_client_secret` are secrets: `repr()` and the UI's launch dialog do not show them.

Three more fields control how rivers waits for a run:

| Field | Default | Description |
|-------|---------|-------------|
| `poll_interval` | `10` | Seconds between status checks of an active run. |
| `poll_timeout` | `None` | Seconds to wait for a run to end. `None` waits until the run ends. |
| `cancel_on_interrupt` | `True` | Cancel the run when rivers stops waiting before the run ends. See [Timeouts and cancel](#timeouts-and-cancel). |

## An asset for a job

```python
import rivers as rs

daily_sales = databricks.job_asset(123, name="daily_sales")

repo = rs.CodeRepository(assets=[daily_sales])
```

`job_asset` does not call Databricks when the code location loads, so you give the asset its `name`. The asset has the kind `databricks`. A run starts the job with `run_now`, waits until the run ends, and records one materialization. A wrong job id fails when the asset runs.

Other keyword arguments go to [`rs.Asset`](../api-reference/assets.md), for example `group`, `partitions_def`, `automation_condition`, `pool`, or `retry`.

`job_parameters` sets the job's [parameters](https://docs.databricks.com/aws/en/jobs/job-parameters) for each run. Give a dict, or a function that gets the asset's context and returns the dict:

```python
from datetime import datetime

daily = rs.PartitionsDefinition.daily(start=datetime(2024, 1, 1))

daily_sales = databricks.job_asset(
    123,
    name="daily_sales",
    partitions_def=daily,
    job_parameters=lambda context: {"date": context.partition_key},
)
```

## An output for each task

```python
sales = databricks.job_asset(456, name="sales", tasks=["orders", "refunds"])
```

With `tasks`, the asset is a [multi-asset](../concepts/assets.md#multi-assets) with one output for each task. Each output has the name of its task key. To give an output another name or other settings, map the task key to an `rs.AssetDef`. An `AssetDef` without a name gets the task key:

```python
sales = databricks.job_asset(
    456,
    name="sales",
    tasks={
        "ingest_orders": rs.AssetDef("raw/orders"),
        "refunds": rs.AssetDef(group="finance"),
    },
)
```

A run of all the outputs runs the whole job. A run of some outputs runs only their tasks: rivers puts their task keys in the `only` field of `run_now`, and Databricks excludes the other tasks. A task that you do not list runs only when all the outputs run.

When the run ends, each selected task that ended with `SUCCESS` records a materialization. The other selected outputs fail with `DatabricksRunError`. When a task that you did not list fails, the outputs do not fail, and the run log has a warning.

rivers does not [retry](../concepts/retries.md#multi-assets) an asset with `tasks`, because its outputs are recorded one at a time. To retry a task, set `max_retries` on the task in Databricks.

## Waiting and failures

While the run is active, rivers gets its state every `poll_interval` seconds. Each new state of the run is logged with the URL of the run page.

A run must end with `SUCCESS`. Any other result raises `DatabricksRunError`, including `SUCCESS_WITH_FAILURES`, `SKIPPED`, and `INTERNAL_ERROR`. The message gives the run's state and the URL of the run page, and the error and stack trace of each task that ended with `FAILED`, `TIMEDOUT`, `CANCELED`, or `INTERNAL_ERROR`:

```text
Databricks run 4417 ended with FAILED: Task ingest failed with message: Workload failed, see run output for details.
https://dbc-1a2b3c4d-5e6f.cloud.databricks.com/jobs/123/runs/4417
Task ingest FAILED: ZeroDivisionError: division by zero
Traceback (most recent call last): ...
```

With `tasks`, the message of a failed output gives the state and the error of the selected tasks that did not succeed.

## Timeouts and cancel

Set `poll_timeout` to stop waiting for long runs:

```python
databricks = DatabricksResource(poll_timeout=3600)
```

When the run does not end within `poll_timeout` seconds, rivers raises `TimeoutError`. A `TimeoutError` has the failure reason `TIMEOUT`, so [`RetryOn.TRANSIENT`](../concepts/retries.md#what-gets-retried-retry_on) retries it.

When rivers stops waiting before the run ends, it cancels the run in Databricks. This occurs after `poll_timeout`, when an API request fails, and on an interrupt such as Ctrl-C. Set `cancel_on_interrupt=False` to let the run continue in Databricks. A process that is killed, for example a deleted Kubernetes pod, cannot cancel the run.

Canceling a rivers run does not cancel a Databricks run that a step waits for. To limit the run on the Databricks side, set `timeout_seconds` in the job.

## Metadata

The materialization of an asset without `tasks` has this metadata:

| Key | Type | Description |
|-----|------|-------------|
| `databricks/run_id` | `int` | The run id. |
| `databricks/job_id` | `int` | The job id. |
| `databricks/run_url` | `MetadataValue.url` | The run page. |
| `databricks/duration` | `MetadataValue.duration` | Seconds from the start of the run to its end, cluster start included. |
| `databricks/output/<task key>` | `str` | The exit value of each notebook task, see below. |

The materialization of each output of an asset with `tasks` has this metadata:

| Key | Type | Description |
|-----|------|-------------|
| `databricks/run_id` | `int` | The run id of the job. |
| `databricks/job_id` | `int` | The job id. |
| `databricks/run_url` | `MetadataValue.url` | The run page of the task. |
| `databricks/duration` | `MetadataValue.duration` | Seconds from the start of the task to its end. |
| `databricks/task_key` | `str` | The task key. |
| `databricks/output` | `str` | The exit value of the task, when it is a notebook task. |

A notebook task can send a value back with `dbutils.notebook.exit()`. Databricks returns the first 5 MB of each value. Keep values small: they are stored as metadata.

```python
# Last cell of the notebook
dbutils.notebook.exit(json.dumps({"rows": 1200}))
```

Python script and wheel tasks have no exit value. Their output stays on the run page.

## Compute and retries

The step of a job asset only starts the run and waits for it. A Databricks cluster or serverless compute does the work: set it in the job or its tasks. On Kubernetes, [`compute=`](../concepts/compute.md) sizes the pod of the waiting step, so a small value is enough. It does not change Databricks compute, and neither does retry escalation.

The function of a job asset is async, so its step waits without holding a worker of the executor, and the job assets of one level wait at the same time: all of them with the parallel executor (`max_async_concurrent` limits them), and up to four with the in-process executor. A waiting step keeps its [pool](../concepts/concurrency.md#concurrency-pools) slots until the run ends. To limit how many Databricks runs a backfill starts at the same time, give the assets a pool:

```python
daily_sales = databricks.job_asset(123, name="daily_sales", partitions_def=daily, pool="databricks")

repo = rs.CodeRepository(assets=[daily_sales], pool_limits={"databricks": 4})
```

An asset with `tasks` takes the pool on each output: `rs.AssetDef(pool="databricks")`.

A rivers retry of an asset without `tasks` starts a new Databricks run. To retry only Databricks failures:

```python
from rivers.integrations.databricks import DatabricksRunError

daily_sales = databricks.job_asset(
    123,
    name="daily_sales",
    retry=rs.RetryPolicy(max_retries=2, retry_on=[DatabricksRunError]),
)
```

Databricks can also retry a task itself: set `max_retries` in the task.

## Run a job in your own asset

`run_job` runs a job of the workspace, and `submit_run` submits a one-time run of tasks that you give in Python. Both wait as `job_asset` does, return a `DatabricksRun`, and raise `DatabricksRunError` when the run does not succeed:

```python
import json

@rs.Asset(kinds="databricks")
def customers(context: rs.AssetExecutionContext) -> rs.Materialization:
    run = databricks.submit_run(
        [
            {
                "task_key": "main",
                "notebook_task": {"notebook_path": "/Workspace/etl/customers"},
                "existing_cluster_id": "0101-123456-abcdefgh",
            }
        ],
        run_name="customers",
        log=context.log,
    )
    rows = json.loads(run.outputs["main"])["rows"]
    return rs.Materialization(metadata={"databricks/run_id": run.run_id, "rows": rows})
```

In an `async def` asset, use `async_run_job` and `async_submit_run`. They take the same arguments and wait without blocking the event loop.

A task is a dict in the form of the [Jobs API](https://docs.databricks.com/api/workspace/jobs/submit), or a `databricks.sdk.service.jobs.SubmitTask`. Other keyword arguments go to `WorkspaceClient.jobs.submit`, for example `run_name`, `timeout_seconds`, `git_source`, or `environments` for serverless tasks.

`get_client()` returns a [`databricks.sdk.WorkspaceClient`](https://databricks-sdk-py.readthedocs.io/en/latest/workspace/index.html) for anything else:

```python
@rs.Asset
def warehouses() -> list[str]:
    return [w.name for w in databricks.get_client().warehouses.list()]
```

See the [API reference](../api-reference/databricks.md) for all the classes.
