# Retries

A step that fails permanently on its first hiccup cascades: every downstream asset is skipped and the only recovery is re-running the whole selection by hand. Declarative retries let a step re-run itself — with a wait schedule, a filter on *which* failures deserve another attempt, and (on Kubernetes) more memory when the failure was an OOM kill.

```python
import rivers as rs

@rs.Asset(retry=rs.RetryPolicy(
    max_retries=3,
    backoff=rs.Backoff.exponential(2.0, max_delay=60, jitter=0.25),
    retry_on=rs.RetryOn.TRANSIENT,
))
def fetch_from_flaky_api() -> dict:
    ...
```

A failed attempt emits a `StepRetry` event (visible in the run timeline), waits out the backoff, and re-executes. Failure hooks fire only when the budget is exhausted — not per attempt. Downstream steps are skipped only if the *final* attempt fails.

## RetryPolicy

```python
rs.RetryPolicy(
    max_retries=3,          # retry budget after the initial attempt
    backoff=...,            # wait schedule; None = retry immediately
    retry_on=...,           # which failures are eligible (default: ALL)
    escalate=...,           # grow compute on OOM retries (Kubernetes)
)
```

The policy attaches at three levels; the **nearest one wins** (total override, no field merging):

```python
@rs.Asset(retry=rs.RetryPolicy(max_retries=5))     # 1. asset (highest)
def flaky(): ...

rs.Job("nightly", assets=[...], retry=...)          # 2. job default for its steps
rs.CodeRepository(..., default_retry_policy=...)    # 3. repo-wide default
repo.materialize([...], retry=...)                  # acts at the job level for that run
```

## Backoff

`Backoff` is built from named constructors — one per wait shape. Every shape takes a relative `jitter` (a fraction of the computed wait, so it scales with the delay) and most take a `max_delay` ceiling so growth can't run away:

```python
rs.Backoff.constant(10)                                    # 10s every time
rs.Backoff.linear(step=30)                                 # 30s, 60s, 90s, …
rs.Backoff.exponential(1.0, max_delay=60, jitter=0.25)     # 1s, 2s, 4s… capped at 60s, ±25%
rs.Backoff.fixed([10, 60, 300])                            # explicit schedule, clamps at 300s
```

The deterministic wait before the attempt that follows the *n*-th failure:

| constructor | wait |
|---|---|
| `constant(delay)` | `delay` |
| `linear(step, initial=0)` | `initial + step * n` |
| `exponential(initial, factor=2)` | `initial * factor**(n-1)` |
| `fixed(schedule)` | `schedule[min(n-1, len-1)]` |

Cap and jitter apply uniformly: `wait = min(computed, max_delay) * uniform(1-jitter, 1+jitter)`.

## What gets retried: `retry_on`

Two presets cover the common intents:

- `RetryOn.ALL` (default) — any failure except cancellation.
- `RetryOn.TRANSIENT` — only out-of-memory, timeout, and infrastructure failures. A deterministic `ValueError` fails identically every attempt; retrying it just burns compute.

For precise control, pass an **allow-list mixing exception types and failure reasons**:

```python
@rs.Asset(retry=rs.RetryPolicy(
    max_retries=4,
    retry_on=[ConnectionError, vendor.RateLimitError, rs.FailureReason.OUT_OF_MEMORY],
))
def call_external(): ...
```

Exception types match subclass-aware, like an `except` clause — listing `ConnectionError` also catches `ConnectionResetError`. `FailureReason` members cover failures that have no exception object at all (an OOM-killed pod leaves nothing to type-match). Cancellation is never retried regardless of policy.

## Named policies

Register policies once on the repository and reference them by name — the same pattern as named IO handlers:

```python
repo = rs.CodeRepository(
    assets=[...],
    retries={
        "flaky_io": rs.RetryPolicy(max_retries=5, backoff=rs.Backoff.exponential(1.0, max_delay=30)),
        "big_data": rs.RetryPolicy(
            max_retries=3,
            retry_on=rs.RetryOn.TRANSIENT,
            escalate=rs.ComputeEscalation(factor=2.0, max_memory="64Gi"),
        ),
    },
    default_retry_policy="flaky_io",
)

@rs.Asset(retry="big_data")
def skewed_join(): ...
```

An unknown name fails at `resolve()` with the registered names listed — never silently at execution time.

## OOM escalation on Kubernetes

On the Kubernetes executor every step runs in its own pod, so a retry can re-launch the step with **more memory**:

```python
@rs.Asset(retry=rs.RetryPolicy(
    max_retries=3,
    retry_on=rs.RetryOn.TRANSIENT,
    escalate=rs.ComputeEscalation(factor=2.0, max_memory="64Gi"),
))
def skewed_join(): ...
```

Starting from the executor's `worker_memory` (say 8Gi), an OOM-killed attempt relaunches at 16Gi, then 32Gi, then 64Gi — clamped at `max_memory`, which is required. A pod killed by the OOM killer never gets to write a failure event; the orchestrator detects the dead Job, reads `OOMKilled` / exit 137 off the pod status, and classifies the failure as `OUT_OF_MEMORY`. `escalate` implies its reasons are retriable, so you don't also have to list `OUT_OF_MEMORY` in `retry_on`.

Each attempt creates a fresh Job (`-r2`, `-r3`, … name suffixes — a finished K8s Job can't re-run). `ComputeEscalation(cpu_factor=..., max_cpu=...)` optionally grows CPU alongside memory.

On the in-process and parallel executors `escalate` is inert — a local Python process can't be handed more RAM than its host has. Timing-based retries work everywhere.

## Executor behavior

- **in_process** — attempts re-run inline; the backoff sleep releases the GIL.
- **parallel** — a failed subprocess step is re-submitted to the worker pool. The exception raised in the worker is re-raised in the orchestrator, so exception-type `retry_on` lists match as usual.
- **kubernetes** — attempts re-create the step Job; classification comes from the step's own failure event when it wrote one, or from pod status when it didn't. The step pod stamps the failure reason and the exception's class hierarchy onto its `StepFailure` event, so exception-type allow-lists match across the pod boundary. A pod killed before it can write an event (e.g. OOM) is classified from pod status alone — only reason-based matching applies there.

Concurrency slots (pools, the executor's step budget) are held per attempt: a non-zero backoff sleep releases them so waiting siblings (or other runs) can proceed, and re-claims them before the next attempt. Cancelling a run interrupts a backoff sleep within about a second.

Retry policies apply to **asset** steps only. Task and bash-task steps (including a graph asset's internal tasks) run without retries — a job-level or repo-default policy skips them, and `resolve()` emits a `UserWarning` naming the affected steps.

## Observability

Every retried attempt leaves a `StepRetry` event on the run timeline carrying the attempt number, the classified failure reason, the next delay, and (when escalating) the next resource request:

| metadata key | example |
|---|---|
| `rivers/attempt` | `1` |
| `rivers/failure_reason` | `out_of_memory` |
| `rivers/next_delay_ms` | `5000` |
| `rivers/next_compute` | `{"memory":"16Gi"}` |

The attempt count for a step is the number of its `StepRetry` events plus one; the final `StepSuccess`/`StepFailure` settles the outcome.

## Multi-assets

A multi-asset is one function call producing several outputs, so it can only retry **as a unit**. `AssetDef(retry=...)` attaches the policy per output, and every output that sets one must set the *same* one (checked at `resolve()`):

```python
@rs.Asset.from_multi(output_defs=[
    rs.AssetDef("orders", retry=rs.RetryPolicy(max_retries=3, retry_on=[ConnectionError])),
    rs.AssetDef("customers"),   # covered by the same step retry
])
def load_tables():
    return {"orders": ..., "customers": ...}
```

Dict-returning multi-assets retry normally. **Generator multi-assets do not retry**: their body runs during output iteration, after earlier yields may already have materialized — re-running the whole generator would double-write those outputs.

## Limitations

- Generator-style multi-assets don't retry (see above); dict-returning ones do.
- Exception-type allow-lists don't match Kubernetes step failures yet (see above).
- A run cancelled mid-backoff finishes the wait before the cancellation is honored at the next step boundary.
