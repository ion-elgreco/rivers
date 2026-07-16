# Retries & Compute

Declarative retry policies and per-asset compute. Concept guides: [Retries](../concepts/retries.md), [Per-asset compute](../concepts/compute.md).

## `RetryPolicy`

Declarative retry policy for a step (asset) or job.

```python
rs.RetryPolicy(
    max_retries=3,
    backoff=rs.Backoff.exponential(2.0, max_delay=60.0),
    retry_on=rs.RetryOn.TRANSIENT,
)
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `max_retries` | `int` | `1` | Retry budget after the initial attempt. |
| `backoff` | `Backoff \| None` | `None` | Wait schedule between attempts (`None` = retry immediately). |
| `retry_on` | `RetryOn \| Sequence[type[BaseException] \| FailureReason] \| None` | `RetryOn.ALL` | A preset, or an allow-list mixing exception types (subclass-aware) and `FailureReason` members. |
| `escalate` | `ComputeEscalation \| None` | `None` | Grow compute on resource-exhaustion retries. |

Attach it per asset (`@rs.Asset(retry=...)`, `AssetDef(retry=...)`), per job (`Job(retry=...)`), per run (`materialize(retry=...)`), or repo-wide (`CodeRepository(default_retry_policy=...)`). Precedence is nearest-wins: asset > job > run/repo default. Every `retry=` site also accepts the name of a policy registered in `CodeRepository(retries={...})`.

---

## `Backoff`

A retry wait schedule. Build via the static factories; every shape takes a relative `jitter` (0..1, fraction of the computed wait) and most take a `max_delay` ceiling in seconds.

| Factory | Wait before attempt *n*+1 |
|---------|---------------------------|
| `Backoff.constant(delay, *, jitter=0.0, max_delay=None)` | `delay` |
| `Backoff.linear(step, *, initial=0.0, jitter=0.0, max_delay=None)` | `initial + step * n` |
| `Backoff.exponential(initial, *, factor=2.0, jitter=0.0, max_delay=None)` | `initial * factor**(n-1)`, capped at `max_delay` |
| `Backoff.fixed(schedule, *, jitter=0.0)` | `schedule[n-1]` (clamped to the last entry) |

All durations are seconds (`float`).

---

## `RetryOn`

Preset failure sets eligible for retry.

| Member | Retries on |
|--------|------------|
| `RetryOn.ALL` | Any non-cancellation failure (default). |
| `RetryOn.TRANSIENT` | Only `OUT_OF_MEMORY` / `TIMEOUT` / `INFRASTRUCTURE` — not deterministic user errors. |

For precise control pass a list instead: `retry_on=[ConnectionError, rs.FailureReason.OUT_OF_MEMORY]`.

---

## `FailureReason`

Why a step failed. Classified per executor (exception type in-process; pod termination reason on Kubernetes) and recorded on `StepFailure` / `StepRetry` events as `rivers/failure_reason`.

| Member | Meaning |
|--------|---------|
| `FailureReason.ERROR` | Ordinary exception raised by user code. |
| `FailureReason.OUT_OF_MEMORY` | OOMKilled / exit 137 / `MemoryError`. |
| `FailureReason.TIMEOUT` | Exceeded a wall-clock deadline / `TimeoutError`. |
| `FailureReason.INFRASTRUCTURE` | Environmental: pod vanished, worker died — no user-code fault. |
| `FailureReason.CANCELLED` | Cancellation requested; never retried. |

---

## `ComputeEscalation`

Grow a step's compute on OOM retries, up to a required ceiling. Effective on executors that provision compute per step (Kubernetes).

```python
rs.ComputeEscalation(factor=2.0, max_memory="64Gi")
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `max_memory` | `str` | required | Hard ceiling on the memory request (k8s quantity, e.g. `"64Gi"`). |
| `factor` | `float` | `2.0` | Multiplier applied to the memory request per escalating retry. |
| `cpu_factor` | `float \| None` | `None` | Optional multiplier for the CPU request. |
| `max_cpu` | `str \| None` | `None` | Ceiling on CPU; used with `cpu_factor`. |
| `on` | `Sequence[FailureReason] \| None` | `[OUT_OF_MEMORY]` | Failure reasons that escalate. |

Setting `escalate` implies the listed reasons are retriable even when `retry_on` doesn't include them.

---

## `Compute`

Per-asset compute resources (Kubernetes quantity strings); `None` on an axis inherits the executor default. Declared per single asset (`@rs.Asset(compute=...)`) or per multi-asset (`Asset.from_multi(compute=...)` — one step is one pod).

```python
@rs.Asset(compute=rs.Compute(cpu="500m", memory="4Gi"))
def heavy(): ...
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `cpu` | `str \| None` | `None` | CPU request/limit (e.g. `"500m"`, `"2"`). |
| `memory` | `str \| None` | `None` | Memory request/limit (e.g. `"4Gi"`). |
| `gpu` | `str \| None` | `None` | GPU count, rendered as `nvidia.com/gpu`. |

Effective on the Kubernetes executor; the in-process and parallel executors log a warning and ignore it. `RetryPolicy.escalate` grows this base on OOM retries.
