# Databricks

## `DatabricksResource`

Runs Databricks jobs, and makes assets that run them. Fields are read from
`DATABRICKS_*` environment variables when not given. See the
[Databricks guide](../guides/databricks.md).

```python
from rivers.integrations.databricks import DatabricksResource

databricks = DatabricksResource(host="https://dbc-1a2b3c4d-5e6f.cloud.databricks.com", token="dapi...")
```

**Constructor:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `host` | `str \| None` | `None` | The workspace URL. |
| `token` | `SecretStr \| None` | `None` | Personal access token. |
| `client_id` | `str \| None` | `None` | OAuth client id of a service principal. |
| `client_secret` | `SecretStr \| None` | `None` | OAuth secret of the service principal. |
| `azure_client_id` | `str \| None` | `None` | Application id of an Azure service principal. |
| `azure_client_secret` | `SecretStr \| None` | `None` | Client secret of the Azure service principal. |
| `azure_tenant_id` | `str \| None` | `None` | Tenant id of the Azure service principal. |
| `poll_interval` | `float` | `10` | Seconds between status checks of an active run. |
| `poll_timeout` | `float \| None` | `None` | Seconds to wait for a run to end. `None` waits until it ends. |
| `cancel_on_interrupt` | `bool` | `True` | Cancel the run when the wait stops before the run ends: after `poll_timeout`, on an error, or on an interrupt such as Ctrl-C. |

The credential fields go to the Databricks SDK's configuration attribute of the same
name. A field that is `None` lets the SDK read its own configuration: environment
variables, `~/.databrickscfg`, or the Azure and Databricks CLIs. The secret fields are
hidden from `repr()` and from the UI's launch dialog.

### Methods

#### `job_asset(job_id, *, name, tasks=None, job_parameters=None, **asset_options)`

Returns an asset that runs job `job_id` of the workspace. The job is not read when the
asset is made: a wrong job id or task key fails when the asset runs.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `job_id` | `int` | required | The job to run. |
| `name` | `str` | required | Asset name. |
| `tasks` | `list[str] \| dict[str, AssetDef] \| None` | `None` | Keys of the tasks that get an output, named like the task. Map a task key to an `AssetDef` to give its output another name or settings; an `AssetDef` without a name gets the task key. |
| `job_parameters` | `dict[str, str] \| (AssetExecutionContext) -> dict[str, str] \| None` | `None` | Values for the job's parameters, or a function that returns them from the asset's context. |
| `**asset_options` | | | Passed to [`Asset`](assets.md) or, with `tasks`, to [`Asset.from_multi()`](assets.md#assetfrom_multi), for example `group`, `partitions_def`, or `retry`. `kinds` defaults to `["databricks"]`. |

Raises `ValueError` when `tasks` is empty, or when two tasks get the same output name.

Without `tasks`, the asset is a `SingleAsset`. A run calls `run_job()` and records a
[`Materialization`](assets.md#materialization) with this metadata:

| Key | Type | Description |
|-----|------|-------------|
| `databricks/run_id` | `int` | The run id. |
| `databricks/job_id` | `int` | The job id. |
| `databricks/run_url` | `MetadataValue.url` | The run page. |
| `databricks/duration` | `MetadataValue.duration` | Seconds from the start of the run to its end. |
| `databricks/output/<task key>` | `str` | One entry for each item of `DatabricksRun.outputs`. |

With `tasks`, the asset is a `MultiAsset` with one output per task. A run of all the
outputs runs every task of the job; a run of some outputs sends their task keys as
`only`. When the run ends, each selected task that ended with `SUCCESS` records a
materialization with this metadata:

| Key | Type | Description |
|-----|------|-------------|
| `databricks/run_id` | `int` | The run id of the job. |
| `databricks/job_id` | `int` | The job id. |
| `databricks/run_url` | `MetadataValue.url` | The run page of the task. |
| `databricks/duration` | `MetadataValue.duration` | Seconds from the start of the task to its end. |
| `databricks/task_key` | `str` | The task key. |
| `databricks/output` | `str` | The value the notebook task passed to `dbutils.notebook.exit()`. |

The other selected outputs fail with `DatabricksRunError`. A failed task without an
output is logged as a warning. rivers does not retry an asset with `tasks`.

#### `run_job(job_id, *, job_parameters=None, only=None, log=None)`

Runs an existing job, waits until the run ends, and returns a [`DatabricksRun`](#databricksrun).

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `job_id` | `int` | required | The job to run. |
| `job_parameters` | `dict[str, str] \| None` | `None` | Values for the job's parameters. |
| `only` | `list[str] \| None` | `None` | Keys of the tasks to run. `None` runs every task. |
| `log` | `logging.Logger \| None` | `None` | Logger for the run's progress, for example `context.log`. |

#### `submit_run(tasks, *, log=None, **submit_args)`

Submits a one-time run of `tasks`, waits until it ends, and returns a
[`DatabricksRun`](#databricksrun).

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `tasks` | `list[jobs.SubmitTask \| dict]` | required | The tasks of the run. A dict has the form of a task in the [Jobs API](https://docs.databricks.com/api/workspace/jobs/submit). |
| `log` | `logging.Logger \| None` | `None` | Logger for the run's progress, for example `context.log`. |
| `**submit_args` | | | Other arguments for `WorkspaceClient.jobs.submit`, for example `run_name`, `timeout_seconds`, `environments`. |

```python
run = databricks.submit_run(
    [
        {
            "task_key": "main",
            "spark_python_task": {"python_file": "/Workspace/etl/customers.py"},
            "existing_cluster_id": "0101-123456-abcdefgh",
        }
    ],
    run_name="customers",
)
```

#### `async_run_job(job_id, *, job_parameters=None, only=None, log=None)`

`run_job` for async code. It takes the same arguments, returns and raises the same, and
waits without blocking the event loop.

#### `async_submit_run(tasks, *, log=None, **submit_args)`

`submit_run` for async code, in the same way:

```python
@rs.Asset(kinds="databricks")
async def customers(context: rs.AssetExecutionContext) -> rs.Materialization:
    run = await databricks.async_submit_run(tasks, run_name="customers", log=context.log)
    return rs.Materialization(metadata={"databricks/run_id": run.run_id})
```

These methods and the job assets get the state of the run every `poll_interval`
seconds, and log each new state. They raise:

| Exception | When |
|-----------|------|
| `DatabricksRunError` | The run ended without `SUCCESS`. |
| `TimeoutError` | The run did not end within `poll_timeout`. |

When the wait stops before the run ends and `cancel_on_interrupt` is set, the run is
canceled. A failed cancel is logged and does not replace the original error.

#### `get_client()`

Returns a `databricks.sdk.WorkspaceClient` for the workspace.

---

## `DatabricksRun`

A Databricks run that ended with `SUCCESS`. Returned by `run_job`, `submit_run`, and
their async versions.
A pydantic model.

| Attribute | Type | Description |
|-----------|------|-------------|
| `run_id` | `int` | The run id. |
| `job_id` | `int \| None` | The job id. A one-time run also has one. |
| `run_page_url` | `str \| None` | The run's page in the workspace. |
| `duration` | `float \| None` | Seconds from the start of the run to its end. |
| `outputs` | `dict[str, str]` | The value each successful notebook task passed to `dbutils.notebook.exit()`, by task key. Databricks returns the first 5 MB of each value. |

---

## `DatabricksRunError`

Raised when a Databricks run, or a selected task of a job asset, ends without
`SUCCESS`. The message gives the run's state, the URL of the run page, and the error
and stack trace of each task that ended with `FAILED`, `TIMEDOUT`, `CANCELED`, or
`INTERNAL_ERROR`. Use it in a retry policy:
`rs.RetryPolicy(max_retries=2, retry_on=[DatabricksRunError])`.
