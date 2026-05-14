# Concurrency Control Flow

rivers has four distinct concurrency control layers, each operating at a different scope.

## Overview

| Layer | Scope | Configured via | Enforced by |
|-------|-------|---------------|-------------|
| **max_concurrent_runs** | How many runs execute at once (global) | `RunQueueConfig.max_concurrent_runs` | Coordinator — won't dequeue if at capacity |
| **Tag concurrency limits** | How many runs with a given tag run at once | `RunQueueConfig.tag_concurrency_limits` | Coordinator — skips queued runs whose tags are at limit |
| **Executor step parallelism** | How many *steps* execute in parallel within one run | `Executor.parallel(max_workers, max_async_concurrent)`, `Executor.kubernetes(max_concurrent_steps)` | Executor — sized worker pool / step-pod scheduler |
| **Pools** | How many *steps* use a resource at once (within/across runs) | `@Asset(pool="X")` + `storage.set_pool_limit("X", N)` | Executor — `PoolGuard` blocks before step execution |

The first two are **run-level** gates (before execution starts). The other two are **step-level** gates (during execution). A run can pass the coordinator's checks but its individual steps still wait for the executor's step budget and pool slots. Executor step parallelism is **per-run** — pools are **global across runs**.

## Flow Diagram

```
                         USER / SCHEDULER / SENSOR
                                   │
                    ┌──────────────┴──────────────┐
                    │                             │
              repo.materialize()           daemon.submit_run()
              (direct execution)           (queued execution)
                    │                             │
                    │                    ┌────────▼────────┐
                    │                    │  RunRecord      │
                    │                    │  status: Queued │
                    │                    │  tags: [...]    │
                    │                    │  priority: N    │
                    │                    └────────┬────────┘
                    │                             │
                    │              ┌──────────────▼──────────────┐
                    │              │   RunQueueCoordinator.tick() │
                    │              │   (daemon polling loop)      │
                    │              ├─────────────────────────────┤
                    │              │                             │
                    │              │  1. GC expired pool leases  │
                    │              │                             │
                    │              │  2. Check max_concurrent_runs│
                    │              │     in_progress >= max?     │
                    │              │     YES → skip cycle        │
                    │              │                             │
                    │              │  3. Tag concurrency limits   │
                    │              │     for each queued run:    │
                    │              │     - count tags of         │
                    │              │       in-progress runs      │
                    │              │     - if run's tags exceed  │
                    │              │       TagConcurrencyLimit   │
                    │              │       → skip (stays Queued) │
                    │              │     - else → dequeue it     │
                    │              │                             │
                    │              │  4. Queued → NotStarted      │
                    │              └──────────┬──────────────────┘
                    │                         │
                    │              daemon picks up NotStarted runs
                    │              and calls materialize(run_id=...)
                    │                         │
                    ├─────────────────────────┘
                    │
                    ▼
         ┌─────────────────────┐
         │  executor.execute_plan()          LEVEL 1         LEVEL 2
         │                     │         ┌──────────┐    ┌──────────┐
         │  steps grouped      │         │ step_a   │    │ step_c   │
         │  by DAG level       │────────▶│ step_b   │───▶│ step_d   │──▶ ...
         │                     │         │ (parallel│    │          │
         │                     │         │  within) │    │          │
         └─────────────────────┘         └────┬─────┘    └──────────┘
                                              │
                          ┌───────────────────┤ for each step:
                          │                   │
                          ▼                   ▼
                    no pool config?     has @Asset(pool="X")?
                    ──────────────      ─────────────────────
                    execute directly    ┌──────────────────────────┐
                                        │  PoolGuard.acquire()     │
                                        │                          │
                                        │  claim_async_poll():     │
                                        │    storage.claim_slots() │
                                        │    Claimed? → continue   │
                                        │    Pending? → sleep+retry│
                                        │      (configurable via   │
                                        │       RIVERS_ env vars)  │
                                        │                          │
                                        │  spawn lease renewal     │
                                        │    (every lease_dur/3)   │
                                        └───────────┬──────────────┘
                                                    │
                                                    ▼
                                             execute the step
                                             (InProcess / loky /
                                              async JoinSet)
                                                    │
                                                    ▼
                                        ┌──────────────────────────┐
                                        │  PoolGuard.release()     │
                                        │  - abort renewal task    │
                                        │  - free_concurrency_slots│
                                        └──────────────────────────┘
                                                    │
                                                    ▼
                                         ┌────────────────────┐
                                         │ end of execute_plan│
                                         │ defense-in-depth:  │
                                         │ free_slots_for_run │
                                         └────────────────────┘
```

## Layer Details

### 1. max_concurrent_runs (Global Run Gate)

The simplest limit: at most N runs execute simultaneously. The coordinator checks `in_progress_count >= max` before dequeuing anything. If at capacity, the entire tick is skipped.

- **Default:** 10
- **Config:** `RunQueueConfig(max_concurrent_runs=10)`
- **Set to -1** for unlimited.

### 2. Tag Concurrency Limits (Per-Tag Run Gate)

Runs carry tags (key-value pairs). Tag concurrency limits restrict how many runs with a specific tag can be in-progress at once. The coordinator scans queued runs in priority order, skipping any whose tags would exceed a limit.

- **Config:** `TagConcurrencyLimit(key="env", value="prod", limit=2)` — at most 2 runs tagged `env:prod`
- **Per-unique-value mode:** `TagConcurrencyLimit(key="tenant", per_unique_value=True, limit=1)` — at most 1 run per distinct tenant value
- Blocked runs stay `Queued` and are re-evaluated on the next tick.

### 3. Executor Step Parallelism (Per-Run Step Cap)

Within a single run, the executor controls how many steps from the same DAG level run in parallel. This is independent from the coordinator (which gates run admission) and from pools (which gate steps that share a resource across runs).

**`Executor.parallel(max_workers, max_async_concurrent)`** — the loky-backed executor.

- `max_workers` (default `os.cpu_count()`) — subprocess pool size for sync steps. Sync steps from the same level fan out across this pool; if the level is wider than `max_workers`, the surplus queues inside the pool until a worker is free.
- `max_async_concurrent` (default unbounded) — cap on concurrently-scheduled async-task steps. Useful when async steps hit a rate-limited service.

**`Executor.kubernetes(..., max_concurrent_steps=N)`** — the K8s-backed executor.

- `max_concurrent_steps` (default unbounded) — cap on concurrently scheduled **step pods** within a single run. Without this, a wide DAG level can saturate node capacity, image-pull bandwidth, or your control-plane API quota.

**`Executor.in_process()`** has no knobs — steps run serially in the calling process (effective parallelism = 1).

These caps **stack with pools** rather than replacing them. If `max_workers=8` and a step claims a pool with `limit=2`, that step is bounded to 2 concurrent instances (pool wins) while other steps from the same level continue filling the remaining 6 worker slots.

The executor cap is also **per-run**, so two runs each with `max_workers=8` can together produce 16 concurrent sync steps. If you need a global step cap across runs, use a pool instead.

### 4. Pools (Step-Level Resource Gate)

Pools limit how many steps use a shared resource concurrently, *within and across runs*. Unlike the run-level gates, pools are enforced during execution by the executor.

**Configuration:**

```python
# On the asset
@Asset(pool="database", pool_slots=2)
def heavy_query(): ...

# On storage (before or during execution)
storage.set_pool_limit("database", limit=5, lease_duration="5m")
```

**Claim protocol:**

- All-or-none atomic claim across multiple pools (multi-pool steps)
- Sentinel transaction prevents concurrent claims from exceeding limits
- Lease-based expiry: crashed processes stop renewing, slots auto-expire
- Background renewal at `lease_duration / 3` intervals via `PoolGuard`
- Run-level cleanup (`free_concurrency_slots_for_run`) at end of execution as defense-in-depth
- Coordinator GC (`free_expired_leases`) on every tick sweeps stale rows

**Environment variables:**

The claim polling loop can be tuned via environment variables. Values are human-readable durations (e.g. `30s`, `5m`, `1h30m`), parsed by the [`humantime`](https://docs.rs/humantime) crate.

| Variable | Default | Description |
|----------|---------|-------------|
| `RIVERS_CLAIM_TIMEOUT` | `10m` | Maximum time a step will wait for pool slots before failing. |
| `RIVERS_CLAIM_POLL_INTERVAL` | `1s` | Base interval between claim attempts. |
| `RIVERS_CLAIM_POLL_JITTER` | `500ms` | Maximum random jitter added per attempt to avoid correlated retries. Set to `0ms` to disable. |

With the defaults, a step waiting for pool slots will timeout after 10 minutes. To allow longer waits (e.g. for long-running batch workloads), increase `RIVERS_CLAIM_TIMEOUT`. To fail faster, lower it.

**Per-backend behavior:**

| Backend | Pool step execution |
|---------|-------------------|
| InProcess | Sequential: claim → execute → release per step |
| Async | Each JoinSet task acquires its own PoolGuard concurrently |
| Parallel | Pool steps run in loky subprocesses via claim-gated JoinSet; non-pool steps use the standard batch pipeline |

Mapped (fan-out) steps claim per instance, not per parent step.

## Observability Events (Phase 3a)

Every concurrency lifecycle transition emits a storage event through the EventWriter pipeline:

| Event | Emitted by | Metadata |
|-------|-----------|----------|
| `RunQueued` | `submit_run`, automation conditions | `priority` |
| `RunDequeued` | Coordinator tick | `priority` |
| `StepSlotClaimed` | PoolGuard (after successful claim) | `pools` (comma-separated) |
| `StepSlotWaiting` | PoolGuard (first pending attempt) | `reason` (block reason) |
| `StepSlotRenewed` | PoolGuard lease renewal task | — |
| `StepSlotReleased` | PoolGuard release | — |

**Block reasons** are persisted on `RunRecord.block_reason` by the coordinator when a queued run is blocked by global run limits or tag concurrency limits. Cleared on dequeue.

## CLI Commands (Phase 3b)

Operators can inspect and manage queues and pools from the terminal:

### Pool commands

| Command | Description |
|---------|-------------|
| `rivers pools list` | List all configured pools with limit, claimed/pending counts, lease duration |
| `rivers pools info <pool>` | Detailed pool info including active slot holders (run_id, step_key, slots, lease expiry) |
| `rivers pools set <pool> <limit>` | Set (upsert) pool slot limit and lease duration |

### Queue commands

| Command | Description |
|---------|-------------|
| `rivers queue list` | List all queued runs sorted by priority (desc) then queue time (asc), with block reason |
| `rivers queue cancel <run_id>` | Cancel a queued run (Queued → Canceled) |
| `rivers queue why <run_id>` | Show why a run is queued: block reason, queue position, priority, tags |

All commands accept `--storage-path` (default `.rivers/storage/`) to specify the embedded storage location.

## UI Dashboards (Phase 3c)

The rivers-ui web interface provides full visibility into concurrency pool state and the run queue.

### Pool Dashboard (`/pools`)

- **Summary stats**: total pools, claimed/total slots, pending step count
- **Pool cards**: each pool shows a utilization bar (green <60%, yellow 60-85%, red >85%), claimed/limit ratio, lease duration, pending count badge
- **Slot holders**: expandable per-pool table showing active holders (run_id, step_key, slots consumed, claimed timestamp, lease expiry with color-coded warning for near-expiry/expired leases)

### Queue View (`/queue`)

- **Queued runs list**: sorted by priority (descending) then queue time (ascending)
- Each run shows queue position, run_id, job name, priority badge, queued-since time, block reason (if blocked), and asset chips
- Auto-refreshes every 5 seconds

### Run Detail — Concurrency Tab

The run detail page (`/runs/:id`) includes a **Concurrency** tab showing:

- **Run-level queue events**: `RunQueued` and `RunDequeued` events with timestamps
- **Per-step concurrency events**: grouped by asset, showing `StepSlotClaimed`, `StepSlotWaiting`, `StepSlotReleased`, and `StepSlotRenewed` events with block reasons
- **Block reason display**: queued runs show their block reason in the run header detail grid
