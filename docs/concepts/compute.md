# Per-asset compute

On the Kubernetes executor every step runs in its own pod, sized by the executor's `worker_cpu` / `worker_memory` — one value for every step in the run. A DAG that mixes a cheap parse step with a 30Gi join shouldn't have to size everything for the join. `compute=` sizes the step per asset:

```python
import rivers as rs

@rs.Asset(compute=rs.Compute(cpu="4", memory="32Gi"))
def skewed_join(ctx):
    ...
```

## Per-axis inheritance

Values are Kubernetes quantity strings (`"4"`, `"500m"`, `"32Gi"`). Any axis left unset inherits the executor default, so "bump only memory" is one field:

```python
repo = rs.CodeRepository(
    assets=[...],
    default_executor=rs.Executor.kubernetes(worker_cpu="1", worker_memory="512Mi"),
)

@rs.Asset(compute=rs.Compute(memory="32Gi"))    # cpu stays "1"
def big(ctx): ...
```

`gpu` requests an extended resource (`nvidia.com/gpu`) on the step pod:

```python
@rs.Asset(compute=rs.Compute(gpu="1", memory="16Gi"))
def train(ctx): ...
```

Requests and limits are set to the same values (Guaranteed QoS), matching how the executor-level knobs behave.

## With retries and escalation

`compute` provides the **base** that [retry escalation](retries.md) grows from: an OOM-killed attempt of the asset above relaunches at `32Gi * factor`, clamped at the policy's `max_memory` — instead of growing from the run-wide `worker_memory`.

```python
@rs.Asset(
    compute=rs.Compute(memory="8Gi"),
    retry=rs.RetryPolicy(
        max_retries=3,
        retry_on=rs.RetryOn.TRANSIENT,
        escalate=rs.ComputeEscalation(factor=2.0, max_memory="64Gi"),
    ),
)
def sometimes_bigger(ctx): ...    # 8Gi → 16Gi → 32Gi → 64Gi
```

## Scope

- Effective on the **Kubernetes** executor. The in-process and parallel executors have no per-step compute envelope; a `compute=` there logs a warning and is ignored.
- Multi-assets: one step is one pod, so `compute` is declared on `Asset.from_multi(compute=...)` itself, not per output:

    ```python
    @rs.Asset.from_multi(
        output_defs=[rs.AssetDef("orders"), rs.AssetDef("customers")],
        compute=rs.Compute(memory="8Gi"),
    )
    def load_tables(): ...
    ```
- Node placement (selectors, tolerations, affinity) and accelerator types are not part of `Compute`; it is resource quantities only.
