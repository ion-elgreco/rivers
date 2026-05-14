# Concurrency & Run Queue

`CodeRepository` accepts three optional configuration objects that govern how runs are queued, where they are launched, and how step-level concurrency pools are sized.

```python
import rivers as rs

repo = rs.CodeRepository(
    assets=[...],
    run_queue=rs.RunQueueConfig(
        max_concurrent_runs=20,
        tag_concurrency_limits=[
            rs.TagConcurrencyLimit(key="team", limit=2, per_unique_value=True),
        ],
        dequeue_interval="500ms",
    ),
    run_backend=rs.RunBackendConfig.kubernetes(image="rivers:1.0"),
    pool_limits={"warehouse": 4, "api": 8},
)
```

For end-to-end behavior (queue → tag gating → pool gating), see the [Concurrency Control Flow](../architecture/concurrency-flow.md) walkthrough.

---

## `RunQueueConfig`

Limits applied by the run-queue dequeuer. Affects daemon-submitted runs (sensors, schedules, automation conditions, UI). Synchronous `repo.materialize()` calls always execute directly, regardless of queue configuration.

```python
queue = rs.RunQueueConfig(
    max_concurrent_runs=10,
    tag_concurrency_limits=[
        rs.TagConcurrencyLimit(key="env", value="prod", limit=3),
    ],
    dequeue_interval="250ms",
)
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `max_concurrent_runs` | `int` | `10` | Maximum number of runs in-flight at once. |
| `tag_concurrency_limits` | `list[TagConcurrencyLimit]` | `[]` | Per-tag concurrency caps applied on top of `max_concurrent_runs`. |
| `dequeue_interval` | `str` | `"250ms"` | Polling interval for the queue worker (humantime). |

---

## `TagConcurrencyLimit`

Cap on concurrent runs that share a tag (or per distinct tag value).

```python
# At most 5 concurrent prod runs
rs.TagConcurrencyLimit(key="env", value="prod", limit=5)

# At most 2 concurrent runs per distinct team value
rs.TagConcurrencyLimit(key="team", limit=2, per_unique_value=True)
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `key` | `str` | required | Tag key the limit applies to. |
| `limit` | `int` | required | Maximum number of concurrent runs. |
| `value` | `str \| None` | `None` | Optional tag value; when omitted, matches all values for `key`. |
| `per_unique_value` | `bool` | `False` | When `True`, apply `limit` independently per distinct value of `key`. Only meaningful when `value` is unset. |

---

## `RunBackendConfig`

Where runs are launched — local subprocesses (default) or Kubernetes pods.

### `RunBackendConfig.local()`

```python
backend = rs.RunBackendConfig.local()
```

Runs jobs in-process or as local subprocesses. The default when `run_backend` is not specified.

### `RunBackendConfig.kubernetes()`

```python
backend = rs.RunBackendConfig.kubernetes(
    image="my-registry/rivers:1.2.3",
    namespace="rivers",
    service_account="rivers-executor",
    run_cpu="500m",
    run_memory="512Mi",
    worker_cpu="500m",
    worker_memory="512Mi",
)
```

Launches each run as a Kubernetes Job and each step as a worker pod (when paired with `Executor.kubernetes()`).

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `image` | `str \| None` | running image | Container image for run/worker pods. |
| `namespace` | `str \| None` | pod's own namespace | Target namespace. |
| `service_account` | `str` | `"rivers-executor"` | Service account bound to the pods. |
| `run_cpu` | `str` | `"500m"` | CPU request/limit for the run pod. |
| `run_memory` | `str` | `"512Mi"` | Memory request/limit for the run pod. |
| `worker_cpu` | `str` | `"500m"` | CPU request/limit for step worker pods. |
| `worker_memory` | `str` | `"512Mi"` | Memory request/limit for step worker pods. |

---

## Concurrency pools

Step-level concurrency pools are declared on the asset (`pool=`, `pool_slots=`) and sized through `CodeRepository(pool_limits={...})` or the `rivers pools set` CLI:

```python
@rs.Asset(pool="warehouse", pool_slots=2)
def heavy_query(...):
    ...

repo = rs.CodeRepository(
    assets=[heavy_query],
    pool_limits={"warehouse": 4},  # 4 total slots; heavy_query consumes 2 each
)
```

A step is admitted only when the pools it claims have free slots. Slots are leased and renewed automatically while the step runs. See [Storage → Pool Methods](storage.md#pool-methods) for runtime inspection (`get_pool_info`, `get_pool_slot_holders`).

The CLI exposes the same operations:

```bash
rivers pools list
rivers pools info warehouse
rivers pools set warehouse 8 --lease-duration 10m
```
