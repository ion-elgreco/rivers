# Concurrency

rivers has four concurrency layers that compose. Each operates at a different scope and answers a different question:

| Layer | Scope | Answers |
|-------|-------|---------|
| **Run queue** | All runs in flight at once | "How many runs total can be running at the same time?" |
| **Tag concurrency limits** | All runs that share a tag | "How many runs *for this team / env / customer* can be running at the same time?" |
| **Executor step parallelism** | Steps inside a single run | "How many steps within one run execute in parallel?" |
| **Concurrency pools** | Individual steps holding a shared resource (global across runs) | "How many *steps* can hit our database / API / external service at once?" |

The first two are **run-level gates** — they decide whether a run is allowed to *start*. The other two are **step-level gates** — they decide whether a step inside an already-running run is allowed to *execute*. A run can clear the queue gates and still have its individual steps wait on the executor's step budget or on pool slots.

Synchronous `repo.materialize()` calls bypass the run-level gates (they run immediately and don't go through the queue), but step-level gates still apply because they're enforced inside the executor for every step.

## Run queue

When `RunQueueConfig` is set on the repository, daemon-submitted runs (sensors, schedules, automation conditions, the UI) go through a queue rather than executing immediately:

```python
import rivers as rs

repo = rs.CodeRepository(
    assets=[...],
    run_queue=rs.RunQueueConfig(
        max_concurrent_runs=10,
        dequeue_interval="250ms",
    ),
)
```

The queue coordinator wakes every `dequeue_interval`, looks at how many runs are in-flight, and admits queued runs up to `max_concurrent_runs`. The rest stay queued. This is the simplest way to keep your daemon (or your data warehouse, or your K8s API) from melting under a burst of triggers.

`materialize()` ignores the queue — call it explicitly only when you mean "run this right now, no gating."

### Launch failures and the start timeout

Dequeuing admits a run (it stops counting as queued and starts counting toward `max_concurrent_runs`) before the executor for it exists. If launching that executor fails, the run is marked failed immediately, with the error attached to the run as a `RunLaunchFailed` event. If the launch is interrupted (say the daemon restarts at exactly the wrong moment), the coordinator's sweep catches it later: a dequeued run with no live executor after `start_timeout` (default 180s) is marked failed the same way. Without this, such a run would hold a concurrency slot forever while being invisible in the queue.

## Tag concurrency limits

Tags carved on runs (via `RunRequest(tags={...})`, `repo.materialize(tags=...)`, or schedule/sensor decorators) become the unit for finer-grained limits. `TagConcurrencyLimit` lets you cap concurrency per tag key, per specific value, or per *distinct* value:

```python
queue = rs.RunQueueConfig(
    max_concurrent_runs=20,
    tag_concurrency_limits=[
        # At most 3 prod runs at once
        rs.TagConcurrencyLimit(key="env", value="prod", limit=3),
        # And independently, at most 2 runs per team (so team A and team B
        # can each have 2 concurrent runs, totalling 4 across the limit)
        rs.TagConcurrencyLimit(key="team", limit=2, per_unique_value=True),
    ],
)
```

These compose with `max_concurrent_runs`: a queued run is admitted only if both checks pass. Tag limits are useful when one tenant or one shared resource needs its own ceiling without blocking everyone else.

## Executor step parallelism

Within a single run, the chosen `Executor` decides how many steps from the same DAG level can execute in parallel. This is independent from the run queue (which gates *runs*) and from pools (which gate *steps that share a resource across the whole system*).

### `Executor.parallel(max_workers, max_async_concurrent)`

- `max_workers` (default: `os.cpu_count()`) — size of the subprocess pool used for sync steps. Sync steps from the same run-level run on this pool. If a run produces more parallel-eligible steps than `max_workers`, the extras queue inside the pool.
- `max_async_concurrent` (default: unbounded) — cap on concurrent async-task steps. Useful when async steps hit a rate-limited service and you don't want every concurrent run-level step launching at once.

```python
default_executor=rs.Executor.parallel(max_workers=8, max_async_concurrent=16)
```

### `Executor.kubernetes(max_concurrent_steps, ...)`

- `max_concurrent_steps` (default: unbounded) — cap on concurrently scheduled **step pods** within one run. Each step is its own pod, so without this cap a wide DAG level can saturate node capacity, image-pull bandwidth, or your control-plane API quota.

```python
default_executor=rs.Executor.kubernetes(
    worker_image="rivers:1.0",
    max_concurrent_steps=20,
)
```

### `Executor.in_process()`

No knobs — steps run serially in the calling process. Per-run step parallelism is effectively 1.

These caps stack with pools rather than replacing them. If `max_workers=8` and a step claims a pool with 2 slots, you can have at most 2 of that step running concurrently (pool wins) but other steps in the same level still fill the other 6 worker slots in parallel.

## Concurrency pools

Pools cap how many **steps** can execute concurrently across all runs. The classic case: one or two database connections, or a rate-limited external API. You don't care how many runs are launched — you care how many steps hit that pool at the same time.

Declare the pool on the asset, set its slot limit at the repository or via the CLI, and the executor enforces it transparently:

```python
@rs.Asset(pool="warehouse", pool_slots=2)   # this asset claims 2 slots when it runs
def heavy_query(...):
    ...

repo = rs.CodeRepository(
    assets=[heavy_query, ...],
    pool_limits={"warehouse": 4},   # 4 total slots — heavy_query can run twice in parallel
)
```

Steps wait until the pool has space, then claim their slots, run, and release on completion. If a step crashes, its slots are released by the lease GC after `lease_duration` so a dead pod can't permanently hold capacity. Multiple pools per asset are supported (pass a list to `pool=`); the step waits for *all* its pools to have space before running.

Pools also expose a CLI (`rivers pools list/info/set`) and are inspectable through the storage backend (`storage.get_all_pool_infos()`, `storage.get_pool_slot_holders(...)`).

## Picking between them

The four layers are not alternatives — most non-trivial deployments use several at once:

- **Run queue** for shedding load: protect the daemon and downstream systems from a thundering herd of triggers.
- **Tag concurrency limits** for tenancy / fairness: stop one team / tenant / env from monopolising the queue.
- **Executor step parallelism** for shaping a single run's footprint: keep one wide DAG level from launching 200 step pods at once, or from spawning more loky workers than your machine can handle.
- **Pools** for shared external resources: cap parallelism on a database, an external API, or anything else where the constraint is per-resource rather than per-run.

When in doubt: if the constraint is *"how many things hit X at once?"* and X is a downstream system shared across runs, use a pool. If the constraint is *"how wide can one run get?"*, use the executor's `max_workers` / `max_concurrent_steps`. If the constraint is *"how many runs of any kind?"* or *"how many runs of this kind?"*, use the queue or a tag limit.

## Where to go next

- [Concurrency & Queue API reference](../api-reference/concurrency.md) — full parameter tables for `RunQueueConfig`, `TagConcurrencyLimit`, `RunBackendConfig`.
- [Concurrency Control Flow](../architecture/concurrency-flow.md) — internal flow diagram, lease semantics, and the observability events emitted at each gate.
- [Storage → Pool methods](../api-reference/storage.md#pool-methods) — runtime inspection (limits, claimed/pending counters, active slot holders).
