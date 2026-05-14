# rivers

**Orchestration platform for tasks and assets, fully backed by Rust.**

rivers is a Rust-powered orchestration platform built around data assets. Define pipelines in Python; rivers resolves the graph, plans execution - no Python interpreter on the control plane.

## Install

```bash
pip install rivers
```

Optional extras for IO handlers:

```bash
pip install rivers[delta]     # Delta Lake support
pip install rivers[pyarrow]   # PyArrow table support
pip install rivers[polars]    # Polars DataFrame support
```

## Quick example

```python
import rivers as rs

@rs.Asset
def raw_data():
    return {"users": 100, "events": 5000}

@rs.Asset
def summary(raw_data: dict):
    return f"{raw_data['users']} users, {raw_data['events']} events"

repo = rs.CodeRepository(assets=[raw_data, summary])
result = repo.materialize()

# Read materialized values via the asset's IO handler:
print(repo.load_node("summary"))  # "100 users, 5000 events"
```

`repo.materialize()` returns a [`RunResult`](api-reference/repository.md#runresult) describing the run; asset values are read back through `repo.load_node(name)`.

## Key features

- **Asset-based orchestration** — define data assets as Python functions; rivers resolves the dependency graph automatically.
- **Rust core** — graph resolution, execution planning, partition logic, and the scheduler all run in compiled Rust.
- **Multiple asset types** — single, multi-output, graph (composing `Task`s into sub-DAGs), and external assets.
- **Partitioning** — static, time-window (daily/hourly/custom cron), multi-dimensional, and runtime-extensible dynamic partitions.
- **Pluggable IO** — built-in handlers for in-memory, pickle (any object store), and Delta Lake with merge support.
- **Parallel & distributed execution** — `Executor.parallel()` for concurrent subprocess workers, `Executor.kubernetes()` for one-pod-per-step on K8s.
- **Schedules, sensors, and automation conditions** — declarative triggers (cron, event-driven, dep-aware) executed by the rivers daemon.
- **Backfills** — partition-range execution with multi-run, single-run, and per-dimension strategies.
- **Persistent storage** — embedded SurrealDB + RocksDB for local dev, SurrealDB server for production.
- **Concurrency control** — run-queue limits, tag concurrency, and step-level concurrency pools.
- **Single-binary dev experience** — `rivers dev <module>` boots SurrealDB (embedded RocksDB), the scheduler, and the web UI on `:3000` in one process.
